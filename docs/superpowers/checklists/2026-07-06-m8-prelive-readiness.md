# M8 Pre-Live Readiness Checklist

Before M8 live integration starts:

- [ ] M7 full test suite passes on `main`.
- [ ] `prodigy-executor --mode live` is still rejected.
- [ ] Telegram `/stop`, `/resume`, `/cancel_all`, and `/close_all` have been tested in demo.
- [ ] SQLite has no unexpected system working orders.
- [ ] SQLite has no unexpected system positions.
- [ ] Recent `events` contain no unresolved `critical` execution errors.
- [ ] M8 has an explicit live-enable design before any live key is used.
- [ ] Live API keys are prepared outside M7 and are not committed.
- [ ] M8 rollback plan is written before small-capital launch.
