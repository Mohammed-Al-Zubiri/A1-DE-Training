#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-${SCRAPING_SERVICE_PORT:-8000}}"

if ! command -v ufw >/dev/null 2>&1; then
    echo "ufw is not installed on this machine."
    exit 0
fi

ufw allow "${PORT}/tcp"
ufw status verbose
