use anyhow::{Context, Result};
use reqwest::Client;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationMode {
    Demo,
    Live,
}

pub fn should_send_telegram_for_mode(mode: NotificationMode, kind: &str) -> bool {
    match mode {
        NotificationMode::Demo => matches!(
            kind,
            "critical"
                | "margin_danger"
                | "manual_override_entered"
                | "manual_override_cleared"
                | "websocket_auth_failed"
                | "rest_order_failed"
        ),
        NotificationMode::Live => matches!(
            kind,
            "fill"
                | "position_closed"
                | "intent_rejected"
                | "critical"
                | "margin_danger"
                | "manual_override_entered"
                | "manual_override_cleared"
                | "websocket_auth_failed"
                | "rest_order_failed"
        ),
    }
}

pub fn should_send_telegram(kind: &str) -> bool {
    should_send_telegram_for_mode(NotificationMode::Demo, kind)
}

pub async fn send_telegram(
    bot_token: Option<&str>,
    chat_id: Option<&str>,
    kind: &str,
    text: &str,
) -> Result<()> {
    if !should_send_telegram(kind) {
        return Ok(());
    }
    let (Some(token), Some(chat)) = (bot_token, chat_id) else {
        return Ok(());
    };
    let url = format!("https://api.telegram.org/bot{token}/sendMessage");
    // ponytail: M4 spec — "Telegram delivery failure must not block execution,
    // order management, or reconcile" (design line 141). reqwest's default has no
    // request timeout, so a hung/slow Telegram POST would block every direct
    // awaiter (reconcile pass, private-WS auth-failure helper). BOUND it to 3s;
    // a timeout returns Err, which every caller already swallows (.ok() / let _ =),
    // so Telegram being down degrades to "no notification" instead of "stuck loop".
    // Single point of fix — no per-caller timeouts.
    let request = Client::new()
        .post(url)
        .form(&[("chat_id", chat), ("text", text)])
        .send();
    let response = tokio::time::timeout(std::time::Duration::from_secs(3), request)
        .await
        .context("telegram send timed out")??;
    response.error_for_status()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_major_events_are_active_notifications() {
        // ponytail: should_send_telegram delegates to Demo mode (third milestone is demo-only),
        // so trade fills are suppressed; only critical/risk/manual-override kinds are active.
        assert!(!should_send_telegram("fill"));
        assert!(should_send_telegram("critical"));
        assert!(should_send_telegram("manual_override_entered"));
        assert!(!should_send_telegram("heartbeat"));
        assert!(!should_send_telegram("info"));
    }

    #[test]
    fn demo_mode_suppresses_normal_trade_notifications_but_allows_manual_override() {
        assert!(!should_send_telegram_for_mode(
            NotificationMode::Demo,
            "fill"
        ));
        assert!(!should_send_telegram_for_mode(
            NotificationMode::Demo,
            "position_closed"
        ));
        assert!(should_send_telegram_for_mode(
            NotificationMode::Demo,
            "manual_override_entered"
        ));
        assert!(should_send_telegram_for_mode(
            NotificationMode::Demo,
            "manual_override_cleared"
        ));
        assert!(should_send_telegram_for_mode(
            NotificationMode::Demo,
            "critical"
        ));
    }

    #[test]
    fn live_mode_sends_trade_and_manual_override_notifications() {
        assert!(should_send_telegram_for_mode(
            NotificationMode::Live,
            "fill"
        ));
        assert!(should_send_telegram_for_mode(
            NotificationMode::Live,
            "position_closed"
        ));
        assert!(should_send_telegram_for_mode(
            NotificationMode::Live,
            "manual_override_entered"
        ));
        assert!(should_send_telegram_for_mode(
            NotificationMode::Live,
            "manual_override_cleared"
        ));
    }

    // Regression for the M4 spec: "Telegram delivery failure must not block
    // execution, order management, or reconcile". send_telegram must never hang
    // the caller. The real-network timeout path can't be exercised without an
    // injectable URL (the host is hard-coded), so this pins the two non-network
    // invariants: (a) a suppressed kind short-circuits to Ok before any I/O, and
    // (b) missing token/chat short-circuits to Ok before any I/O. Both guarantee
    // the most common callers (every demo-kind in notify, and any M4 daemon run
    // without configured Telegram creds) return promptly. The 3s send() bound is
    // a one-line constant verified by inspection + the live daemon smoke.
    #[tokio::test]
    async fn send_telegram_short_circuits_without_hanging() {
        // Suppressed kind: no I/O attempted.
        let suppressed = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            send_telegram(Some("t"), Some("c"), "info", "x"),
        )
        .await
        .expect("suppressed kind must not hang");
        assert!(suppressed.is_ok());

        // Missing token: no I/O attempted even for an active kind.
        let no_token = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            send_telegram(None, None, "critical", "x"),
        )
        .await
        .expect("missing-creds must not hang");
        assert!(no_token.is_ok());

        // Active kind, partial creds (no chat): still short-circuits.
        let partial = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            send_telegram(Some("t"), None, "critical", "x"),
        )
        .await
        .expect("partial-creds must not hang");
        assert!(partial.is_ok());
    }
}
