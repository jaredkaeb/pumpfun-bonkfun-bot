"""
AI Strategy Manager integration.

Hooks the trading bot into a sibling project at ../ai-strategy-manager/ that
runs a Claude-powered analyst loop. The bot writes trades + skips + API events
to a shared SQLite DB, and reads strategy parameters from a shared JSON config
that the analyst mutates between cycles.
"""
