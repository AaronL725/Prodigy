# M8 Demo To Live Switch

1. In demo, run `/stop`.
2. Run `/cancel_all`.
3. If `/status` shows system positions, run `/close_all` and confirm.
4. Run `/status`.
5. Confirm:
   - no pending or accepted intents;
   - no pending or accepted control commands;
   - no working system orders;
   - no system positions;
   - mode is still `MODE DEMO`.
6. Stop the demo executor cleanly.
7. Add live credentials to `.env.local` or the process environment:
   - `BITGET_LIVE_API_KEY`
   - `BITGET_LIVE_API_SECRET`
   - `BITGET_LIVE_API_PASSPHRASE`
8. Set:
   - `PRODIGY_LIVE_TRADING_ENABLED=1`
   - `PRODIGY_LIVE_CONFIRM=I_UNDERSTAND_THIS_CAN_TRADE_REAL_MONEY`
9. Start:
   `prodigy-executor --mode live --daemon`
10. Run `/status` and verify `MODE LIVE`.

If the DB is not clean, live startup must fail even if this checklist is wrong.
