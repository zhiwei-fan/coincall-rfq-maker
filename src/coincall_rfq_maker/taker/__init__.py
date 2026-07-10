"""Live taker CLI package. The `execute` and `trade` commands place REAL block trades.

Safety rails live in `cli.py`: taker-only credentials (never the maker's), a refusal to
run against a non-beta REST host without --allow-prod, and a typed confirmation plus a
hard notional cap on anything that trades.
"""
