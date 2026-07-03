use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashSet;

use crate::bitget::BitgetRestClient;
use crate::db;
use crate::manual_override::{apply_exchange_intervention, ExchangeIntervention, InterventionKind};
use crate::notify;
use crate::types::{OrderRecord, PositionRecord};

pub fn classify_position(
    mut exchange_position: PositionRecord,
    local_order_intents: &HashSet<String>,
    now: &str,
) -> PositionRecord {
    // System-owned if we can trace it to a local order/intent. The exchange
    // all-position response doesn't carry our source_intent_id, so reconcile
    // sets it before calling this (from the local orders table). If it's set
    // and matches a local intent, it's system; otherwise imported.
    if exchange_position
        .source_intent_id
        .as_ref()
        .map(|id| local_order_intents.contains(id))
        .unwrap_or(false)
    {
        exchange_position.ownership = "system".to_string();
    } else {
        exchange_position.ownership = "imported".to_string();
        exchange_position.adopted_at = Some(now.to_string());
        exchange_position.opened_at = Some(now.to_string());
    }
    exchange_position
}

/// Classify a single exchange position as system vs imported, tracing ownership
/// from the LOCAL ORDERS TABLE (not the positions row). The exchange all-position
/// response never carries our source_intent_id, so on the first reconcile that
/// sees a freshly-opened position the positions row has no source_intent_id yet
/// — a position-source-based classify would mark our own position imported and
/// spuriously enter manual override. The orders trace (an unclosed system open
/// cycle) is the reliable signal: if found, the position is system-owned and its
/// source_intent_id is backfilled from that order. Otherwise imported/manual.
fn classify_exchange_position(
    conn: &rusqlite::Connection,
    mut exchange_position: PositionRecord,
    now: &str,
) -> Result<PositionRecord> {
    let traced = system_ownership_intent(conn, &exchange_position.symbol)?;
    match traced {
        Some(intent_id) => {
            exchange_position.source_intent_id = Some(intent_id);
            exchange_position.ownership = "system".to_string();
        }
        None => {
            exchange_position.ownership = "imported".to_string();
            exchange_position.adopted_at = Some(now.to_string());
            exchange_position.opened_at = Some(now.to_string());
        }
    }
    Ok(exchange_position)
}

/// Determine whether an exchange position for this symbol is system-owned.
/// Uses the SIGNED NET BASE our filled system orders imply (buy +, sell −): if
/// the net is non-zero the system still holds exposure it opened, so the current
/// exchange position is system-owned. Order-count heuristics (opens > closes)
/// misjudge partial closes and multi-open/single-close cycles; net base is the
/// only reliable signal. Returns the intent_id of the most recent open order (to
/// backfill source_intent_id) when the net is non-zero, else None.
pub fn system_ownership_intent(
    conn: &rusqlite::Connection,
    symbol: &str,
) -> Result<Option<String>> {
    use rusqlite::params;
    // System-owned iff our filled orders net to a non-zero base for this symbol.
    let (net_base, _side) = db::system_net_base_for_symbol(conn, symbol)?;
    if net_base.abs() <= DUST_BASE {
        return Ok(None);
    }
    let intent_id: Option<String> = conn
        .query_row(
            "select intent_id from orders
             where symbol = ? and intent_id is not null and action = 'open'
             order by updated_at desc limit 1",
            params![symbol],
            |row| row.get(0),
        )
        .ok()
        .flatten();
    Ok(intent_id)
}

/// Classify a manual change to a SYSTEM-owned position by comparing the net base
/// our local orders imply (system_expected_base) against the exchange position's
/// size/side. Any unexplained drift = a client touched the position in the Bitget
/// UI (add/reduce/close/reverse), which must enter manual override so auto-open
/// pauses. Returns None when exchange and system are in sync.
///
/// Signs: system_expected_base and exchange_size are signed long+ / short-; sides
/// are "long"/"short"/"" (no position). Drifts below the dust floor are ignored so
/// sub-min-qty rounding doesn't spuriously trip override.
fn classify_position_drift(
    system_expected_base: f64,
    exchange_size: f64,
    system_side: &str,
    exchange_side: &str,
) -> Option<InterventionKind> {
    let drift = exchange_size - system_expected_base;
    if drift.abs() <= DUST_BASE {
        return None;
    }
    let same_side = exchange_side == system_side && !exchange_side.is_empty();
    if !same_side {
        // Side changed (flip or closed-to-zero): a manual close+reverse or full close.
        return Some(InterventionKind::Close);
    }
    if drift > 0.0 {
        Some(InterventionKind::Add)
    } else {
        Some(InterventionKind::Reduce)
    }
}

const DUST_BASE: f64 = 1e-6;

/// Verdict for a local 'submitted' system order that the exchange no longer lists
/// as pending. Reconcile second-confirms via GET /api/v2/mix/order/detail before
/// acting: the fillList can lag/truncate, so a just-filled order may briefly look
/// "gone from pending" and must NOT be misjudged as a manual cancel.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MissingOrderVerdict {
    /// The order actually filled (fully or partially) — reconcile the fill/order,
    /// this is NOT a manual cancel. Carries the observed filled base.
    Filled(f64),
    /// Confirmed cancelled/rejected on the exchange with NO fill — mark
    /// externally_cancelled and enter override.
    Cancelled,
    /// Cancelled/rejected AFTER a partial fill (baseVolume > 0). The partial base
    /// is REAL system exposure and must be synced (filled_size) before the order
    /// is retired — treating it as a pure Cancelled would drop the partial
    /// position and break ownership tracking. Carries the observed filled base.
    CancelledWithPartialFill(f64),
    /// Neither filled nor confirmed cancelled (still live, or detail unreadable).
    /// Do NOT mark cancelled — leave the order needs_reconcile and emit an event
    /// so a later pass / human reconciles it.
    Unknown,
}

/// Classify a missing-from-pending system order from its exchange order-detail
/// `data` object (GET /api/v2/mix/order/detail). Pure wrapper over the same
/// status/filled parse the execution loop uses, so a filled-but-not-pending order
/// is honored instead of misjudged as a manual cancel. `order_size` is the local
/// ordered size (to tell full vs partial fill); passes 0.0 to skip the size check.
pub fn classify_missing_pending_order(
    detail: &serde_json::Value,
    order_size: f64,
) -> MissingOrderVerdict {
    let (status, filled) = crate::executor::read_detail_fields(detail);
    match crate::executor::classify_order_poll(&status, filled, order_size) {
        crate::executor::OrderPollOutcome::Filled => MissingOrderVerdict::Filled(filled),
        crate::executor::OrderPollOutcome::Vanished => {
            // Cancelled/rejected. If it parted-filled first (baseVolume > 0), that
            // partial base is real system exposure — sync it before retiring, don't
            // treat as a pure cancel.
            if filled > 0.0 {
                MissingOrderVerdict::CancelledWithPartialFill(filled)
            } else {
                MissingOrderVerdict::Cancelled
            }
        }
        crate::executor::OrderPollOutcome::Live => {
            // Some base filled but still working, OR no readable status/fill.
            if filled > 0.0 {
                MissingOrderVerdict::Filled(filled)
            } else {
                MissingOrderVerdict::Unknown
            }
        }
    }
}

/// Decide whether a single fillList row from GET /api/v2/mix/order/fills is a
/// missing local fill worth repairing, and if so build its FillRecord. The
/// exchange fillList carries `orderId` (the exchange order id) but NOT our
/// `clientOid`, so we join on order_id: a fill is ours iff its orderId is a
/// local order we placed, and it's new iff its trade_id isn't already recorded.
/// `client_oid_for` resolves the local client_oid for that order_id (for the FK).
/// A fill with no baseVolume is skipped (nothing to record). Pure so the dedup
/// logic is testable without a network round-trip.
fn fill_to_repair(
    row: &serde_json::Value,
    local_order_ids: &HashSet<String>,
    existing_trade_ids: &HashSet<String>,
    client_oid_for: impl Fn(&str) -> Option<String>,
) -> Option<crate::types::FillRecord> {
    use crate::types::FillRecord;
    let order_id = str_field(row, "orderId");
    if order_id.is_empty() || !local_order_ids.contains(&order_id) {
        return None;
    }
    let trade_id = str_field(row, "tradeId");
    if !trade_id.is_empty() && existing_trade_ids.contains(&trade_id) {
        return None;
    }
    let size: f64 = row
        .get("baseVolume")
        .and_then(serde_json::Value::as_str)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0);
    if size <= 0.0 {
        return None;
    }
    let price: f64 = row
        .get("price")
        .and_then(serde_json::Value::as_str)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0);
    let fee: f64 = row
        .get("feeDetail")
        .and_then(serde_json::Value::as_array)
        .and_then(|arr| arr.first())
        .and_then(|d| d.get("totalFee"))
        .and_then(serde_json::Value::as_str)
        .and_then(|v| v.parse::<f64>().ok())
        .map(|f| f.abs())
        .unwrap_or(0.0);
    Some(FillRecord {
        // fill_id keyed by trade_id so the insert-or-ignore PK dedupes across runs.
        fill_id: if trade_id.is_empty() {
            format!("fill-{order_id}-{}", str_field(row, "cTime"))
        } else {
            format!("fill-{trade_id}")
        },
        order_id: order_id.clone(),
        trade_id: if trade_id.is_empty() {
            None
        } else {
            Some(trade_id)
        },
        client_oid: client_oid_for(&order_id),
        symbol: str_field(row, "symbol"),
        side: str_field(row, "side"),
        price,
        size,
        fee,
        created_at: str_field(row, "cTime"),
        raw_json: row.to_string(),
    })
}

pub async fn reconcile_once(
    conn: &Connection,
    rest: &BitgetRestClient,
    now: &str,
    detect_override: bool,
    telegram_token: Option<&str>,
    telegram_chat: Option<&str>,
) -> Result<()> {
    // ponytail: WS is the fast path; this REST pass repairs anything it missed.
    // Exchange state wins on conflict (spec). We INSERT missing orders/positions
    // and refresh position fields from exchange truth; we do not delete local rows
    // the exchange no longer lists (a filled/cancelled order's terminal state is
    // already persisted by the execution loop).
    let local_oids = db::local_order_client_oids(conn)?;
    let mut repaired_orders = 0u32;
    let mut repaired_positions = 0u32;
    let symbol = rest.display_symbol().to_string();
    let override_key = format!("manual_override:{symbol}");
    let mut override_active = matches!(
        db::get_executor_state(conn, &override_key)?.as_deref(),
        Some("active")
    );
    // Guard: an override entered on THIS pass must not be undone by the same-pass
    // auto-clear (a manual full-close leaves position=0 and orders=0, which would
    // otherwise clear the override we just entered for that very close).
    let mut entered_this_run = false;
    let mut exchange_open_count: usize = 0;
    // Whether the exchange returned a position row for our symbol this pass. If it
    // did NOT but our local orders still imply a nonzero net, the position was
    // fully closed outside the executor (manual full-close) → manual override.
    let mut exchange_has_position = false;
    // client_oids the exchange still lists as pending — a local system pending
    // order NOT in this set was cancelled outside the executor (manual cancel).
    let mut exchange_pending_client_oids: HashSet<String> = HashSet::new();

    // Open orders: insert any exchange order we don't already have locally.
    let open_orders = rest
        .get(
            "/api/v2/mix/order/orders-pending",
            &[
                ("productType", rest.product_type().to_string()),
                ("marginCoin", rest.margin_coin().to_string()),
            ],
        )
        .await?;
    for row in open_orders
        .get("data")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        let client_oid = str_field(&row, "clientOid");
        if row.get("symbol").and_then(serde_json::Value::as_str) != Some(rest.bitget_symbol()) {
            continue;
        }
        exchange_open_count += 1;
        if !client_oid.is_empty() {
            exchange_pending_client_oids.insert(client_oid.clone());
        }
        if client_oid.is_empty() || local_oids.contains(&client_oid) {
            continue;
        }
        // An exchange order we did NOT place (no local client_oid) = manual
        // intervention. Enter per-symbol override (persisted) so auto-open pauses.
        // Skipped in test-reset mode (system cleanup, not user intervention).
        if detect_override && !override_active {
            let kind = intervention_kind_for_side(&str_field(&row, "side"));
            let mut state = override_state_from(override_active);
            if let crate::manual_override::ManualOverrideDecision::Entered(sym) =
                apply_exchange_intervention(
                    &mut state,
                    ExchangeIntervention {
                        symbol: symbol.clone(),
                        matched_local_client_oid: false,
                        kind,
                    },
                )
            {
                db::set_executor_state(conn, &override_key, "active")?;
                override_active = true;
                entered_this_run = true;
                db::write_event(
                    conn,
                    "warning",
                    "executor",
                    "manual override entered",
                    &format!("{{\"symbol\":\"{sym}\"}}"),
                )?;
                notify::send_telegram(
                    telegram_token,
                    telegram_chat,
                    "manual_override_entered",
                    &format!("manual override entered for {sym}"),
                )
                .await
                .ok();
            }
        }
        let order_id = str_field(&row, "orderId");
        let order = OrderRecord {
            order_id: order_id.clone(),
            exchange_order_id: Some(order_id),
            client_oid: client_oid.clone(),
            intent_id: None,
            symbol: rest.display_symbol().to_string(),
            side: str_field(&row, "side"),
            action: str_field(&row, "tradeSide"),
            order_type: str_field(&row, "orderType"),
            status: str_field(&row, "status"),
            price: row
                .get("price")
                .and_then(serde_json::Value::as_str)
                .and_then(|v| v.parse().ok()),
            size: row
                .get("size")
                .and_then(serde_json::Value::as_str)
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0),
            filled_size: row
                .get("baseVolume")
                .or_else(|| row.get("accBaseVolume"))
                .and_then(serde_json::Value::as_str)
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0),
            attempt: 1,
            raw_json: row.to_string(),
            last_error: None,
        };
        db::upsert_order(conn, &order)?;
        repaired_orders += 1;
    }

    // Fills: repair any exchange fill for one of our orders we haven't recorded
    // yet (the execution loop syncs filled_size/status as it polls, but the
    // per-trade fills ledger comes only from fillList here; a crash/restart or
    // missed run can leave trades un-persisted). Source = the exchange
    // fillList (GET /api/v2/mix/order/fills); join on orderId (the fillList has
    // no clientOid), dedup by trade_id, insert-or-ignore. This runs BEFORE the
    // position/drift loop below so a repaired fill's order sync is visible to
    // same-pass drift detection (else the system net still reads 0 and drift
    // mis-fires manual override on our own already-filled position).
    let existing_trade_ids = db::local_fill_trade_ids(conn)?;
    let local_order_ids = db::local_order_id_to_client_oid(conn)?;
    let local_order_id_set: HashSet<String> = local_order_ids.keys().cloned().collect();
    let mut repaired_fills = 0u32;
    let fills = rest
        .get(
            "/api/v2/mix/order/fills",
            &[
                ("productType", rest.product_type().to_string()),
                ("symbol", rest.bitget_symbol().to_string()),
                ("limit", "50".to_string()),
            ],
        )
        .await?;
    for row in fills
        .get("data")
        .and_then(|d| d.get("fillList"))
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        if let Some(fill) = fill_to_repair(&row, &local_order_id_set, &existing_trade_ids, |oid| {
            local_order_ids.get(oid).cloned()
        }) {
            let order_id = fill.order_id.clone();
            db::insert_fill(conn, &fill)?;
            // Sync the parent order's status/filled_size so system_net_base sees
            // the repaired fill. Without this a crash-then-repair leaves the order
            // 'submitted'/filled_size=0, the system net stays 0, and reconcile
            // mis-fires manual-override drift on our own (already-filled) position.
            db::sync_order_fill_state(conn, &order_id)?;
            repaired_fills += 1;
        }
    }

    // Manual CANCEL detection: a system order we still hold as 'submitted' that
    // the exchange no longer lists as pending was EITHER cancelled outside the
    // executor (manual cancel in the Bitget UI) OR filled but briefly absent from
    // the pending list (the fillList can lag/truncate). Second-confirm via order
    // detail before acting, so a just-filled order is honored, not misjudged as a
    // manual cancel:
    //   Filled/partial → sync filled_size/status (no override).
    //   Cancelled      → externally_cancelled + enter override.
    //   Unknown        → needs_reconcile + event, do NOT mark cancelled.
    // Skipped in test-reset mode (system cleanup, not user intervention).
    if detect_override {
        for (client_oid, order_id, ordered_size) in db::local_working_system_orders(conn, &symbol)?
        {
            if exchange_pending_client_oids.contains(&client_oid) {
                continue; // still live on the exchange — not cancelled.
            }
            let detail = rest.get_order_detail(&client_oid).await.ok();
            let verdict = match &detail {
                Some(d) => classify_missing_pending_order(d, ordered_size),
                None => MissingOrderVerdict::Unknown,
            };
            // Helper: enter the per-symbol override once (idempotent within a pass).
            // Returns true iff this call actually transitioned inactive→active.
            let mut enter_override = || -> Result<bool> {
                if override_active {
                    return Ok(false);
                }
                db::set_executor_state(conn, &override_key, "active")?;
                override_active = true;
                entered_this_run = true;
                Ok(true)
            };
            match verdict {
                MissingOrderVerdict::Filled(filled) => {
                    // The order actually filled — sync the order, NOT a cancel. The
                    // detail's baseVolume is CUMULATIVE, so it must NOT be written as a
                    // fills row (that would pollute the per-trade ledger and double-count
                    // once the real fillList arrives). Set orders.filled_size/status
                    // directly; the fills table is repaired separately from fillList.
                    db::set_order_filled_from_detail(conn, &order_id, filled)?;
                }
                MissingOrderVerdict::CancelledWithPartialFill(filled) => {
                    // Partially filled THEN cancelled: the partial base (e.g. 0.02 of
                    // 0.05) is REAL system exposure. Sync filled_size FIRST so
                    // system_net_base/system_ownership_intent track it, then retire the
                    // order as externally_cancelled and enter override. The fills table
                    // is still only written by fillList (no synthetic row here).
                    db::set_order_filled_from_detail(conn, &order_id, filled)?;
                    db::mark_order_externally_cancelled(conn, &client_oid)?;
                    let did_enter = enter_override()?;
                    db::write_event(
                        conn,
                        "warning",
                        "executor",
                        "manual override entered (order externally cancelled after partial fill)",
                        &format!(
                            "{{\"symbol\":\"{symbol}\",\"client_oid\":\"{client_oid}\",\"filled\":{filled}}}"
                        ),
                    )?;
                    if did_enter {
                        notify::send_telegram(
                            telegram_token,
                            telegram_chat,
                            "manual_override_entered",
                            &format!(
                                "manual override entered for {symbol} (order externally cancelled)"
                            ),
                        )
                        .await
                        .ok();
                    }
                }
                MissingOrderVerdict::Cancelled => {
                    db::mark_order_externally_cancelled(conn, &client_oid)?;
                    let did_enter = enter_override()?;
                    db::write_event(
                        conn,
                        "warning",
                        "executor",
                        "manual override entered (order externally cancelled)",
                        &format!("{{\"symbol\":\"{symbol}\",\"client_oid\":\"{client_oid}\"}}"),
                    )?;
                    if did_enter {
                        notify::send_telegram(
                            telegram_token,
                            telegram_chat,
                            "manual_override_entered",
                            &format!(
                                "manual override entered for {symbol} (order externally cancelled)"
                            ),
                        )
                        .await
                        .ok();
                    }
                }
                MissingOrderVerdict::Unknown => {
                    // Don't claim a cancel — flag it for a later pass / human.
                    let _ = conn.execute(
                        "update orders set status = 'needs_reconcile', updated_at = datetime('now')
                         where client_oid = ?",
                        rusqlite::params![client_oid],
                    );
                    db::write_event(
                        conn,
                        "warning",
                        "executor",
                        "system order missing from exchange pending; left to reconcile",
                        &format!("{{\"symbol\":\"{symbol}\",\"client_oid\":\"{client_oid}\"}}"),
                    )?;
                }
            }
        }
    }

    // Positions: classify (system vs imported) and upsert exchange truth.
    let positions = rest
        .get(
            "/api/v2/mix/position/all-position",
            &[
                ("productType", rest.product_type().to_string()),
                ("marginCoin", rest.margin_coin().to_string()),
            ],
        )
        .await?;
    for row in positions
        .get("data")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        let symbol = rest.display_symbol().to_string();
        let size = row
            .get("total")
            .or_else(|| row.get("available"))
            .and_then(serde_json::Value::as_str)
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);
        let entry = row
            .get("averageOpenPrice")
            .or_else(|| row.get("openPriceAvg"))
            .and_then(serde_json::Value::as_str)
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);
        let upnl = row
            .get("unrealizedPL")
            .or_else(|| row.get("unrealizedPnl"))
            .and_then(serde_json::Value::as_str)
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);
        // Trace ownership from the LOCAL ORDERS TABLE (a position-source-based
        // classify misjudges a freshly-opened position that has no
        // source_intent_id on the positions row yet as imported).
        let record = PositionRecord {
            symbol,
            side: str_field(&row, "holdSide"),
            notional: size.abs() * entry,
            entry_price: entry,
            unrealized_pnl: upnl,
            ownership: "system".to_string(),
            opened_at: None,
            adopted_at: None,
            source_intent_id: None,
            raw_json: row.to_string(),
        };
        let record = classify_exchange_position(conn, record, now)?;
        // Remember the exchange returned a live position for our symbol, so the
        // post-loop manual-full-close check (which fires when NO row comes back)
        // knows this symbol still has exposure on the exchange.
        if record.symbol == rest.display_symbol() && size.abs() > DUST_BASE {
            exchange_has_position = true;
        }
        db::upsert_position(conn, &record)?;
        repaired_positions += 1;

        // A position we can't trace to a local intent = manual intervention
        // (e.g. operator market-opened in the Bitget client). Enter override.
        // Skipped in test-reset mode (system cleanup, not user intervention).
        if detect_override && record.ownership == "imported" && !override_active {
            db::set_executor_state(conn, &override_key, "active")?;
            override_active = true;
            entered_this_run = true;
            db::write_event(
                conn,
                "warning",
                "executor",
                "manual override entered (imported position)",
                &format!("{{\"symbol\":\"{}\"}}", record.symbol),
            )?;
            notify::send_telegram(
                telegram_token,
                telegram_chat,
                "manual_override_entered",
                &format!(
                    "manual override entered for {} (imported position)",
                    record.symbol
                ),
            )
            .await
            .ok();
        }

        // A SYSTEM-owned position whose exchange size/side doesn't match the net
        // base our local orders imply = a client manually added, reduced, closed,
        // or reversed it in the Bitget UI. Imported-position detection can't see
        // this (the position IS traced to a local order), so compare the exchange
        // size against the local net and enter override on any drift. Skipped in
        // test-reset mode.
        if detect_override && record.ownership == "system" && !override_active {
            let (sys_base, sys_side) = db::system_net_base_for_symbol(conn, &record.symbol)?;
            let exchange_signed = if record.side == "short" { -size } else { size };
            if let Some(kind) =
                classify_position_drift(sys_base, exchange_signed, sys_side, &record.side)
            {
                db::set_executor_state(conn, &override_key, "active")?;
                override_active = true;
                entered_this_run = true;
                db::write_event(
                    conn,
                    "warning",
                    "executor",
                    "manual override entered (position drift)",
                    &format!(
                        "{{\"symbol\":\"{}\",\"kind\":\"{:?}\"}}",
                        record.symbol, kind
                    ),
                )?;
                notify::send_telegram(
                    telegram_token,
                    telegram_chat,
                    "manual_override_entered",
                    &format!(
                        "manual override entered for {} (position drift: {:?})",
                        record.symbol, kind
                    ),
                )
                .await
                .ok();
            }
        }
    }

    // Manual FULL-CLOSE detection: when a client fully closes a system position in
    // the Bitget UI, the exchange stops returning a position row for the symbol, so
    // the per-row drift check above never runs. Detect it here: our local orders
    // still imply a nonzero system net base, but the exchange returned no position
    // for the symbol → the position was closed out from under us. ALWAYS sync the
    // local state to exchange truth (retire the contributing orders + clear the
    // local position row) — even when override is already active from a prior
    // reduce/add — so the net base returns to zero and the next pass won't
    // clear-then-re-enter override (flapping). Only ENTER override if it isn't
    // already active. Skipped in test-reset mode (system cleanup, not user
    // intervention).
    if detect_override && !exchange_has_position {
        let (sys_base, _sys_side) = db::system_net_base_for_symbol(conn, &symbol)?;
        if sys_base.abs() > f64::EPSILON {
            db::mark_system_orders_externally_closed(conn, &symbol)?;
            // Exchange state wins: the exchange no longer holds this position, so
            // remove the local positions row too — otherwise local /positions and
            // PnL queries keep reporting a position Bitget closed.
            db::clear_local_position(conn, &symbol)?;
            if !override_active {
                db::set_executor_state(conn, &override_key, "active")?;
                override_active = true;
                entered_this_run = true;
                db::write_event(
                    conn,
                    "warning",
                    "executor",
                    "manual override entered (position externally closed)",
                    &format!("{{\"symbol\":\"{symbol}\"}}"),
                )?;
                notify::send_telegram(
                    telegram_token,
                    telegram_chat,
                    "manual_override_entered",
                    &format!("manual override entered for {symbol} (position externally closed)"),
                )
                .await
                .ok();
            } else {
                // Override already active (e.g. from a prior reduce/add); still
                // record that the position was externally closed this pass.
                db::write_event(
                    conn,
                    "warning",
                    "executor",
                    "position externally closed while override active",
                    &format!("{{\"symbol\":\"{symbol}\"}}"),
                )?;
            }
        }
    }

    // Auto-clear the per-symbol override once the exchange has no position and no
    // open orders for it (spec: resume auto-open when pos+orders reach zero). Skip
    // when we ENTERED override this same pass: a manual full-close/cancel leaves
    // pos+orders at zero, which would otherwise clear the override we just set
    // before the operator ever sees it. It clears on a later pass instead.
    if override_active && !entered_this_run {
        let exchange_position_notional: f64 = {
            let pos = rest
                .get(
                    "/api/v2/mix/position/all-position",
                    &[
                        ("productType", rest.product_type().to_string()),
                        ("marginCoin", rest.margin_coin().to_string()),
                    ],
                )
                .await?;
            pos.get("data")
                .and_then(serde_json::Value::as_array)
                .map(|rows| {
                    rows.iter()
                        .filter(|r| {
                            r.get("symbol").and_then(serde_json::Value::as_str)
                                == Some(rest.bitget_symbol())
                        })
                        .filter_map(|r| {
                            r.get("total")
                                .or_else(|| r.get("available"))
                                .and_then(serde_json::Value::as_str)
                                .and_then(|v| v.parse::<f64>().ok())
                        })
                        .map(|v| v.abs())
                        .sum::<f64>()
                })
                .unwrap_or(0.0)
        };
        let mut state = override_state_from(true);
        if let crate::manual_override::ManualOverrideDecision::Cleared(sym) =
            crate::manual_override::maybe_clear_manual_override(
                &mut state,
                &symbol,
                exchange_position_notional,
                exchange_open_count,
            )
        {
            db::set_executor_state(conn, &override_key, "cleared")?;
            db::write_event(
                conn,
                "info",
                "executor",
                "manual override cleared",
                &format!("{{\"symbol\":\"{sym}\"}}"),
            )?;
            notify::send_telegram(
                telegram_token,
                telegram_chat,
                "manual_override_cleared",
                &format!("manual override cleared for {sym}"),
            )
            .await
            .ok();
        }
    }

    let summary = format!(
        "{{\"repaired_orders\":{repaired_orders},\"repaired_positions\":{repaired_positions},\"repaired_fills\":{repaired_fills}}}"
    );
    db::write_event(
        conn,
        "info",
        "executor",
        "reconciliation completed",
        &summary,
    )?;
    Ok(())
}

fn str_field(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// Map an exchange order side to a manual-intervention kind for the override gate.
fn intervention_kind_for_side(side: &str) -> InterventionKind {
    match side {
        "buy" => InterventionKind::Open,
        "sell" => InterventionKind::Close,
        _ => InterventionKind::Open,
    }
}

/// Build the in-memory override state from the persisted flag. The detection
/// functions need a ManualOverrideState; we seed it from executor_state so the
/// "already blocked → NoChange" path holds across restarts.
fn override_state_from(active: bool) -> crate::manual_override::ManualOverrideState {
    let mut s = crate::manual_override::ManualOverrideState::default();
    if active {
        s.enter("ETH/USDT:USDT");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PositionRecord;

    #[test]
    fn exchange_position_without_local_order_is_imported() {
        let local_order_intents = std::collections::HashSet::new();
        let exchange = PositionRecord {
            symbol: "ETH/USDT:USDT".to_string(),
            side: "long".to_string(),
            notional: 1000.0,
            entry_price: 3000.0,
            unrealized_pnl: 0.0,
            ownership: "system".to_string(),
            opened_at: None,
            adopted_at: None,
            source_intent_id: None,
            raw_json: "{}".to_string(),
        };

        let adopted = classify_position(exchange, &local_order_intents, "2026-07-01T00:00:00Z");

        assert_eq!(adopted.ownership, "imported");
        assert_eq!(adopted.adopted_at.as_deref(), Some("2026-07-01T00:00:00Z"));
    }

    #[test]
    fn fill_to_repair_only_inserts_missing_fills_for_local_orders() {
        // Reconcile repairs fills from the exchange fillList, which carries
        // orderId but NOT clientOid. It must insert a fill only when (a) its
        // orderId is one of OUR orders, and (b) we don't already have its
        // trade_id; otherwise it stays deduped.
        use std::collections::HashSet;
        let mut local_order_ids = HashSet::new();
        local_order_ids.insert("oid-7".to_string());
        let mut existing = HashSet::new();
        existing.insert("trade-already".to_string());
        // Resolve our client_oid for a local order_id (for the fills FK).
        let client_oid_for = |oid: &str| {
            if oid == "oid-7" {
                Some("client-7".to_string())
            } else {
                None
            }
        };

        // A fill for our order, not yet recorded → repair it, with real price/fee.
        let row = serde_json::json!({
            "tradeId": "trade-new", "orderId": "oid-7",
            "symbol": "ETHUSDT", "side": "buy", "price": "1800", "baseVolume": "0.05",
            "cTime": "1783010850639",
            "feeDetail": [{"totalFee": "-0.09"}],
        });
        let rec = fill_to_repair(&row, &local_order_ids, &existing, client_oid_for)
            .expect("should repair");
        assert_eq!(rec.order_id, "oid-7");
        assert_eq!(rec.client_oid.as_deref(), Some("client-7"));
        assert_eq!(rec.trade_id.as_deref(), Some("trade-new"));
        assert_eq!(rec.size, 0.05);
        assert_eq!(rec.price, 1800.0);
        assert_eq!(rec.fee, 0.09); // abs value

        // Same trade_id we already have → skip (dedup).
        let dup = serde_json::json!({
            "tradeId": "trade-already", "orderId": "oid-7",
            "symbol": "ETHUSDT", "side": "buy", "price": "1800", "baseVolume": "0.05",
            "cTime": "1",
        });
        assert!(fill_to_repair(&dup, &local_order_ids, &existing, client_oid_for).is_none());

        // A fill for someone else's order (orderId not local) → skip.
        let foreign = serde_json::json!({
            "tradeId": "trade-x", "orderId": "oid-x",
            "symbol": "ETHUSDT", "side": "buy", "price": "1800", "baseVolume": "0.05",
            "cTime": "1",
        });
        assert!(fill_to_repair(&foreign, &local_order_ids, &existing, client_oid_for).is_none());

        // A fill with no baseVolume → nothing to record → skip.
        let empty = serde_json::json!({
            "tradeId": "trade-z", "orderId": "oid-7",
            "symbol": "ETHUSDT", "side": "buy", "price": "1800", "baseVolume": "0",
            "cTime": "1",
        });
        assert!(fill_to_repair(&empty, &local_order_ids, &existing, client_oid_for).is_none());
    }

    #[test]
    fn fill_to_repair_skips_trades_already_recorded() {
        // Dedup guard: when the same exchange fillList trade is seen across
        // reconcile passes (re-fetch, or a trade that arrived earlier), fill_to_repair
        // must NOT re-insert it — otherwise sync_order_fill_state (SUM(fills)) would
        // double-count and over-state the position. Dedup is by tradeId in the
        // existing set. (The execution path no longer writes fills at all — fills
        // come only from fillList — so this guards reconcile-vs-reconcile re-runs.)
        use std::collections::HashSet;
        let mut local_order_ids = HashSet::new();
        local_order_ids.insert("oid-7".to_string());
        // A prior reconcile pass already recorded trade "T-exec" for this order.
        let mut existing = HashSet::new();
        existing.insert("T-exec".to_string());
        let client_oid_for = |_: &str| Some("client-7".to_string());

        // fillList arrives again with the SAME tradeId already recorded → skip.
        let same_trade = serde_json::json!({
            "tradeId": "T-exec", "orderId": "oid-7",
            "symbol": "ETHUSDT", "side": "buy", "price": "1800", "baseVolume": "0.05",
            "cTime": "1",
        });
        assert!(fill_to_repair(&same_trade, &local_order_ids, &existing, client_oid_for).is_none());

        // A DIFFERENT tradeId not yet recorded → repair it.
        let new_trade = serde_json::json!({
            "tradeId": "T-new", "orderId": "oid-7",
            "symbol": "ETHUSDT", "side": "buy", "price": "1800", "baseVolume": "0.03",
            "cTime": "2",
        });
        let rec = fill_to_repair(&new_trade, &local_order_ids, &existing, client_oid_for)
            .expect("a new trade should be repaired");
        assert_eq!(rec.trade_id.as_deref(), Some("T-new"));
    }

    #[test]
    fn no_double_count_when_fill_list_arrives_after_execution_set_filled_size() {
        // G1/G2 unified rule: the execution path must NOT write a fills row from
        // order-detail cumulative baseVolume — it only sets orders.filled_size.
        // fills come per-trade from fillList via reconcile. Verify the full flow
        // doesn't double-count: execution observes baseVolume 0.05 (no tradeId —
        // detail is order-level), then fillList returns the two real trades
        // (0.02 + 0.03). Final orders.filled_size must be 0.05 and sum(fills) 0.05,
        // NOT 0.10 (which the old execution-writes-fills path produced).
        let conn = exec_db();
        insert_intent(&conn, "i1", "open");
        // Execution path places an order then sets filled_size from detail (0.05),
        // WITHOUT writing any fills row (the fixed behavior).
        insert_order(&conn, "o1", "i1", "buy", "open", "submitted", 0.05);
        db::set_order_filled_from_detail(&conn, "o1", 0.05).unwrap();
        let filled_size: f64 = conn
            .query_row(
                "select filled_size from orders where order_id='o1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!((filled_size - 0.05).abs() < 1e-9);
        // No fills row from the execution path.
        assert_eq!(
            conn.query_row::<i64, _, _>(
                "select count(*) from fills where order_id='o1'",
                [],
                |r| { r.get(0) }
            )
            .unwrap(),
            0
        );

        // Later, fillList arrives with the two real per-trade rows for this order.
        let mut local_order_ids = std::collections::HashSet::new();
        local_order_ids.insert("o1".to_string());
        let existing = db::local_fill_trade_ids(&conn).unwrap();
        for (tid, vol) in [("T1", 0.02), ("T2", 0.03)] {
            let row = serde_json::json!({
                "tradeId": tid, "orderId": "o1",
                "symbol": "ETHUSDT", "side": "buy", "price": "1800",
                "baseVolume": vol.to_string(), "cTime": "1",
            });
            let fill = fill_to_repair(&row, &local_order_ids, &existing, |_| {
                Some("c1".to_string())
            })
            .expect("each trade should be repaired");
            db::insert_fill(&conn, &fill).unwrap();
        }
        // sum(fills) reflects only the two real trades — the execution path added
        // no cumulative row, so there's no double-count.
        let sum_fills: f64 = conn
            .query_row(
                "select coalesce(sum(size),0) from fills where order_id='o1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            (sum_fills - 0.05).abs() < 1e-9,
            "sum(fills) must be 0.05 (the two real trades), got {sum_fills}"
        );
        // And sync_order_fill_state keeps filled_size at 0.05, not 0.10.
        db::sync_order_fill_state(&conn, "o1").unwrap();
        let filled_size_after: f64 = conn
            .query_row(
                "select filled_size from orders where order_id='o1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            (filled_size_after - 0.05).abs() < 1e-9,
            "filled_size must stay 0.05 after fillList repair, got {filled_size_after}"
        );
    }

    #[test]
    fn exchange_position_traced_from_local_order_is_system_without_position_row() {
        // The bug this guards: reconcile used to build the classify set from
        // positions.source_intent_id. But the exchange all-position response
        // never carries our intent_id, and on the FIRST reconcile that sees a
        // freshly-opened position the positions row has NO source_intent_id yet.
        // Ownership must be traced from the orders table (a filled/submitted
        // open order whose intent is unmatched by a close) — so a position with
        // no source_intent_id on the positions row still classifies as system.
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(include_str!("../../../schema/001_initial.sql"))
            .unwrap();
        conn.execute_batch(include_str!("../../../schema/002_execution.sql"))
            .unwrap();
        // Local open order already filled for intent-7; no close order, so there
        // is an unclosed system opening cycle for this symbol.
        conn.execute(
            "insert into trade_intents (
               intent_id, created_at, symbol, side, action, target_notional,
               max_order_notional, status, source
             ) values ('intent-7', '2026-07-01T00:00:00Z', 'ETH/USDT:USDT',
               'long', 'open', 100, 100, 'executed', 'test')",
            [],
        )
        .unwrap();
        conn.execute(
            "insert into orders (
               order_id, client_oid, intent_id, symbol, side, action, order_type,
               status, price, size, filled_size, created_at, updated_at
             ) values ('oid-7', 'client-7', 'intent-7', 'ETH/USDT:USDT', 'buy',
               'open', 'limit', 'filled', 3000, 0.01, 0.01,
               '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();
        // No positions row at all → local_system_intent_ids() (the buggy source)
        // returns an EMPTY set, so a position-source-based classify would mark
        // this imported. The orders trace must win.
        let exchange = PositionRecord {
            symbol: "ETH/USDT:USDT".to_string(),
            side: "long".to_string(),
            notional: 30.0,
            entry_price: 3000.0,
            unrealized_pnl: 0.0,
            ownership: "system".to_string(),
            opened_at: None,
            adopted_at: None,
            source_intent_id: None,
            raw_json: "{}".to_string(),
        };

        let classified =
            classify_exchange_position(&conn, exchange, "2026-07-01T00:00:00Z").unwrap();

        assert_eq!(classified.ownership, "system");
        assert_eq!(classified.source_intent_id.as_deref(), Some("intent-7"));
        assert_eq!(classified.adopted_at, None);
    }

    fn exec_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(include_str!("../../../schema/001_initial.sql"))
            .unwrap();
        conn.execute_batch(include_str!("../../../schema/002_execution.sql"))
            .unwrap();
        conn
    }

    fn insert_intent(conn: &rusqlite::Connection, intent_id: &str, action: &str) {
        conn.execute(
            "insert into trade_intents (intent_id, created_at, symbol, side, action,
               target_notional, max_order_notional, status, source)
             values (?1, '2026-07-01T00:00:00Z', 'ETH/USDT:USDT', 'long', ?2, 100, 100,
               'executed', 't')",
            rusqlite::params![intent_id, action],
        )
        .unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_order(
        conn: &rusqlite::Connection,
        order_id: &str,
        intent_id: &str,
        side: &str,
        action: &str,
        status: &str,
        filled: f64,
    ) {
        conn.execute(
            "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values (?1, ?1, ?2, 'ETH/USDT:USDT', ?3, ?4, 'limit', ?5,
               3000, ?6, ?6, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            rusqlite::params![order_id, intent_id, side, action, status, filled],
        )
        .unwrap();
    }

    #[test]
    fn ownership_uses_net_base_not_order_count() {
        // Old bug: system_ownership_intent used open_count > close_count. Two filled
        // opens then a single filled close that zeroes the net base would be judged
        // system-owned (2 opens > 1 close) even though the system holds nothing —
        // and conversely a partial close (1 open, 1 close, net still positive) would
        // be judged NOT system (1 == 1) even though a position remains. Ownership
        // must follow the signed net base, not the order counts.
        let conn = exec_db();
        insert_intent(&conn, "i-open", "open");
        insert_intent(&conn, "i-close", "close");

        // Two opens (0.05 + 0.05 = 0.10) and one close (0.10) → net 0 → NOT system.
        insert_order(&conn, "o1", "i-open", "buy", "open", "filled", 0.05);
        insert_order(&conn, "o2", "i-open", "buy", "open", "filled", 0.05);
        insert_order(&conn, "o3", "i-close", "sell", "close", "filled", 0.10);
        assert_eq!(
            system_ownership_intent(&conn, "ETH/USDT:USDT").unwrap(),
            None,
            "net base is zero, so the position is not system-owned"
        );

        // Partial close: reopen 0.10, close only 0.04 → net +0.06 → system-owned.
        let conn = exec_db();
        insert_intent(&conn, "i-open", "open");
        insert_intent(&conn, "i-close", "close");
        insert_order(&conn, "o1", "i-open", "buy", "open", "filled", 0.10);
        insert_order(&conn, "o2", "i-close", "sell", "close", "filled", 0.04);
        assert_eq!(
            system_ownership_intent(&conn, "ETH/USDT:USDT")
                .unwrap()
                .as_deref(),
            Some("i-open"),
            "net base is still positive after a partial close, so system-owned"
        );
    }

    #[test]
    fn missing_pending_order_second_confirmed_via_detail() {
        // D2: a 'submitted' system order missing from the exchange pending list is
        // NOT assumed cancelled — the fillList can lag/truncate, so a just-filled
        // order may briefly look "gone". Reconcile second-confirms via order detail:
        // filled/partial → honor the fill; cancelled → externally_cancelled;
        // live/unknown → needs_reconcile, never marked cancelled.

        // Confirmed cancelled on the exchange → Cancelled.
        let cancelled = serde_json::json!({"state":"canceled","baseVolume":"0"});
        assert_eq!(
            classify_missing_pending_order(&cancelled, 0.05),
            MissingOrderVerdict::Cancelled
        );

        // PARTIAL fill THEN cancelled (baseVolume 0.02 < ordered 0.05, status
        // canceled): the 0.02 is REAL system exposure — must NOT be treated as a
        // pure Cancelled (that would drop the partial position). Returns the
        // partial base so the caller syncs filled_size before retiring the order.
        let partial_cancelled = serde_json::json!({"state":"canceled","baseVolume":"0.02"});
        assert_eq!(
            classify_missing_pending_order(&partial_cancelled, 0.05),
            MissingOrderVerdict::CancelledWithPartialFill(0.02)
        );

        // Same but with the "status" spelling instead of "state".
        let partial_cancelled2 = serde_json::json!({"status":"cancelled","baseVolume":"0.02"});
        assert_eq!(
            classify_missing_pending_order(&partial_cancelled2, 0.05),
            MissingOrderVerdict::CancelledWithPartialFill(0.02)
        );

        // Actually filled (status filled, full base) → Filled, NOT Cancelled.
        let filled = serde_json::json!({"state":"filled","baseVolume":"0.05"});
        assert_eq!(
            classify_missing_pending_order(&filled, 0.05),
            MissingOrderVerdict::Filled(0.05)
        );

        // Partial fill but still 'live' with base>0 → honor the partial fill.
        let partial = serde_json::json!({"state":"live","baseVolume":"0.02"});
        assert_eq!(
            classify_missing_pending_order(&partial, 0.05),
            MissingOrderVerdict::Filled(0.02)
        );

        // Still live, no fill, not cancelled → Unknown (leave to reconcile).
        let live = serde_json::json!({"state":"live","baseVolume":"0"});
        assert_eq!(
            classify_missing_pending_order(&live, 0.05),
            MissingOrderVerdict::Unknown
        );

        // Unreadable detail (no status/fill) → Unknown, not a silent cancel.
        let unreadable = serde_json::json!({});
        assert_eq!(
            classify_missing_pending_order(&unreadable, 0.05),
            MissingOrderVerdict::Unknown
        );
    }

    #[test]
    fn partial_fill_then_cancel_is_tracked_in_system_net_base() {
        // Issues 1+2: an order that partially filled (0.02) then was cancelled
        // ends up 'externally_cancelled' with filled_size 0.02. That partial base
        // is REAL system exposure — system_net_base must count it (keyed on
        // filled_size, status-agnostic) so ownership tracking and drift detection
        // see the position, instead of dropping it as if it never filled.
        let conn = exec_db();
        insert_intent(&conn, "i1", "open");
        conn.execute(
            "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values ('o1','o1','i1','ETH/USDT:USDT','buy','open','limit','externally_cancelled',
               3000, 0.05, 0.02, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();
        let (net, side) = db::system_net_base_for_symbol(&conn, "ETH/USDT:USDT").unwrap();
        assert!(
            (net - 0.02).abs() < 1e-9 && side == "long",
            "partial-fill-then-cancel must still count toward system net, got {net}/{side}"
        );

        // And system_ownership_intent must still see the symbol as system-owned
        // (nonzero net) so the position isn't mis-classified imported.
        assert_eq!(
            system_ownership_intent(&conn, "ETH/USDT:USDT")
                .unwrap()
                .as_deref(),
            Some("i1")
        );
    }

    #[test]
    fn position_drift_classifies_manual_add_reduce_close() {
        // A system-owned position should match the net base our local orders
        // imply (filled opens minus filled closes). A client who manually adds,
        // reduces, or closes the position drives a drift between the two that no
        // local order explains — that drift must enter manual override, not be
        // silently adopted as the new system size.

        // In sync: exchange size equals local net → no intervention.
        assert_eq!(classify_position_drift(0.10, 0.10, "long", "long"), None);
        // Manual close/reduce-to-zero: system expected 0.10 but exchange has 0.
        assert_eq!(
            classify_position_drift(0.10, 0.0, "long", ""),
            Some(InterventionKind::Close)
        );
        // Manual partial reduce: exchange smaller than system expected.
        assert_eq!(
            classify_position_drift(0.10, 0.06, "long", "long"),
            Some(InterventionKind::Reduce)
        );
        // Manual add: exchange larger than system expected.
        assert_eq!(
            classify_position_drift(0.10, 0.14, "long", "long"),
            Some(InterventionKind::Add)
        );
        // Side flipped (long→short): a manual reverse = close+open; treat as a
        // manual change so override engages.
        assert_eq!(
            classify_position_drift(0.10, 0.02, "long", "short"),
            Some(InterventionKind::Close)
        );
    }

    #[test]
    fn exchange_position_matching_local_intent_is_system() {
        // Positive branch: an exchange position whose source_intent_id is in the
        // local set is system-owned (NOT imported). Catches a regression toward
        // unwrap_or(true)/is_some() that would silently reclassify everything.
        let mut local_order_intents = std::collections::HashSet::new();
        local_order_intents.insert("intent-7".to_string());
        let exchange = PositionRecord {
            symbol: "ETH/USDT:USDT".to_string(),
            side: "long".to_string(),
            notional: 1000.0,
            entry_price: 3000.0,
            unrealized_pnl: 0.0,
            ownership: "system".to_string(),
            opened_at: Some("2026-06-01T00:00:00Z".to_string()),
            adopted_at: None,
            source_intent_id: Some("intent-7".to_string()),
            raw_json: "{}".to_string(),
        };

        let adopted = classify_position(exchange, &local_order_intents, "2026-07-01T00:00:00Z");

        assert_eq!(adopted.ownership, "system");
        // system-owned keeps its prior opened_at; adoption timestamp not overwritten.
        assert_eq!(adopted.adopted_at, None);
    }
}
