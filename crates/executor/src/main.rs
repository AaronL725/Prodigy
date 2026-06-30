mod db;
mod types;

use anyhow::{bail, Result};
use rusqlite::Connection;

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let db_path = match args.next().as_deref() {
        Some("--db") => args.next().unwrap_or_else(|| "var/prodigy.sqlite".to_string()),
        Some(other) => bail!("unknown argument: {other}"),
        None => "var/prodigy.sqlite".to_string(),
    };

    let conn = Connection::open(db_path)?;
    let intents = db::pending_intents(&conn)?;
    for intent in intents {
        db::reject_intent(
            &conn,
            &intent.intent_id,
            "dry executor rejects intents until Bitget execution is implemented",
        )?;
        println!("rejected {}", intent.intent_id);
    }
    Ok(())
}
