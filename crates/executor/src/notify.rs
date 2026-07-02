use anyhow::Result;
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
    Client::new()
        .post(url)
        .form(&[("chat_id", chat), ("text", text)])
        .send()
        .await?
        .error_for_status()?;
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
}
