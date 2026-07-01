use anyhow::Result;
use reqwest::Client;

pub fn should_send_telegram(kind: &str) -> bool {
    matches!(
        kind,
        "fill" | "position_closed" | "intent_rejected" | "critical" | "margin_danger"
    )
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
        assert!(should_send_telegram("fill"));
        assert!(should_send_telegram("critical"));
        assert!(!should_send_telegram("heartbeat"));
        assert!(!should_send_telegram("info"));
    }
}
