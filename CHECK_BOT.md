# Bot status & control — quick reference

The bot is running in the background. You can close Claude Code and it will
keep running. To check on it or stop it, open Terminal and run these commands.

## Current session
- **PID stored at:** `/tmp/bot_pid.txt`
- **Log file:** `logs/day3_overnight.log`
- **Strategy:** momentum_v1 (smart-snipe filters)
- **Position size:** 0.003 SOL (~$0.25)
- **Filters:** skip_pumped (>1.5x launch) + min 2 SOL real liquidity + rugcheck + curve checks

## Is it still running?

```bash
ps -p $(cat /tmp/bot_pid.txt) && echo "still running" || echo "stopped"
```

## See latest activity

```bash
cd "/Users/jaredkaeb/Documents/Projects and Apps/pumpfun-bonkfun-bot"
tail -50 logs/day3_overnight.log
```

## How many trades + what's the score?

```bash
# Count buys, fails, exits
grep -c "Successfully bought" logs/day3_overnight.log
grep -c "Successfully exited position" logs/day3_overnight.log
grep -c "Failed to exit position" logs/day3_overnight.log

# Latest true-PnL lines
grep "True PnL" logs/day3_overnight.log | tail -10
```

## See all trades in the database

```bash
sqlite3 "/Users/jaredkaeb/Documents/Projects and Apps/ai-strategy-manager/strategy_manager.db" "SELECT id, substr(token_address,1,12), entered_at, exit_reason, printf('%.4f',pnl_usd), printf('%.1f',pnl_percent) FROM trades WHERE id > 76 ORDER BY id;" -header -column
```

## Wallet balance NOW

```bash
curl -s "https://mainnet.helius-rpc.com/?api-key=f0e5bad2-588f-45ec-aafe-2146079dbe0f" -X POST -H "Content-Type: application/json" -d '{"jsonrpc":"2.0","id":1,"method":"getBalance","params":["HUK4sF7uMYspXQAgFfkv6L22DEcsxcsYcKMvdJAt1rBy"]}' | python3 -c "import sys,json; r=json.load(sys.stdin); sol=r['result']['value']/1e9; print(f'{sol:.6f} SOL = \${sol*84:.2f}')"
```

## STOP the bot

```bash
kill $(cat /tmp/bot_pid.txt)
```

## Run a Strategy Manager review (once you have a handful of trades)

```bash
cd "/Users/jaredkaeb/Documents/Projects and Apps/ai-strategy-manager"
source .venv/bin/activate
python -m src.main --once
```

This calls Claude to analyze what the bot did and propose tuning changes.

## When to come back

- After **a few hours** if you want to see real signal
- **Immediately** if the wallet balance drops more than $3 below where you left it
- **Whenever curious** — running 1 cycle of the Strategy Manager is ~$0.20 and shows you what Claude thinks
