# Security policy

## Supported versions

Only the latest release (currently 0.2.x) receives security fixes.

## Reporting a vulnerability

Please **do not open a public issue**. Report privately via GitHub:
**Security → Report a vulnerability** on <https://github.com/co-codin/USDT-Tracker/security/advisories/new>.

Include the affected version, configuration (without secrets) and steps to reproduce.
You should get a reply within 7 days; fixes are released as a patch version with a
GitHub security advisory.

## Scope notes

USDT-Tracker is read-only: it never holds private keys or signs transactions. Relevant
issues include leaking secrets (API tokens, webhook secrets, bot tokens, database URLs),
authentication bypass on `/v1`, SSRF via webhook/RPC URLs, missed or forged transfers,
and denial of service through the Telegram bot or HTTP API.
