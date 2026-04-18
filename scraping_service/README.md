# Scraping Service

This project implements a full Data Engineering mini-system:
1. Scrape data from https://books.toscrape.com/
2. Clean and normalize records
3. Store records in SQLite
4. Expose data through FastAPI
5. Run as a background Ubuntu systemd service
6. Package as an installable .deb

## Project Layout

- src/scraping_service: app source code
- scripts: local development entrypoints
- deploy/systemd: systemd service unit
- deploy/config: default production environment file
- deploy/ufw: firewall helper script
- packaging/debian: Debian package metadata and scripts
- packaging/build_deb.sh: package builder
- docs/system-analysis.pdf: Handwritten system analysis including design choices and end-to-end data flow.
- docs/data-flow.md: architecture and data flow explanation

## Local Run (Development)

1. Install Python dependencies:

   pip install -r requirements.txt

2. Run a one-shot scrape:

   python scripts/run_scrape_once.py

3. Start the API:

   python scripts/run_api.py

4. Test endpoints:

   GET  /health
   GET  /books?page=1&page_size=20
   GET  /books/{book_id}
   GET  /stats/categories
   GET  /stats/ratings
   POST /scrape/trigger
   
## Firewall

Use deploy/ufw/open-port.sh to open the API port (default 8000):

sudo bash deploy/ufw/open-port.sh 8000

## Linux Service

Systemd unit file is at deploy/systemd/scraping-service.service.
It starts /usr/bin/scraping-service-api, runs as user scraping-service,
and auto-restarts on failure.

## Build Debian Package

Run on Ubuntu:

bash packaging/build_deb.sh

Output:

packaging/build/scraping-service_<version>.deb

Install:

sudo dpkg -i packaging/build/scraping-service_1.0.0-1.deb
sudo systemctl status scraping-service
