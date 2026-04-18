# Data Flow and Design Rationale

## End-to-End Data Flow

1. Extract
- Module: src/scraping_service/scraper.py
- Reads category pages from Books to Scrape.
- Collects raw fields from HTML cards: title, price text, rating text, availability text, category, links.

2. Transform/Clean
- Module: src/scraping_service/cleaner.py
- Converts raw price text to float.
- Maps rating strings (One..Five) to integers.
- Normalizes availability text and whitespace.
- Generates deterministic IDs from product URLs.
- Deduplicates by product URL.

3. Store
- Module: src/scraping_service/database.py
- Creates SQLite schema and indexes.
- Upserts records into books table to prevent duplicates.

4. Serve
- Module: src/scraping_service/api.py
- FastAPI endpoints read from SQLite for list/detail/statistics.
- Manual refresh endpoint triggers fresh scrape cycle.

5. Operate
- Service unit: deploy/systemd/scraping-service.service
- Runs API in background and starts on boot.
- Restarts automatically on failure.

6. Secure and Deploy
- Firewall helper: deploy/ufw/open-port.sh.
- Debian packaging: packaging/debian + packaging/build_deb.sh.

## Why This Design

- Modular separation: scraper, cleaner, DB, and API are isolated for easier testing and maintenance.
- SQLite persistence: simple, portable, and ideal for single-node service packaging.
- FastAPI layer: gives a clear network interface for consumers and grading.
- systemd + .deb: matches production-style Linux operations while staying lightweight.

## Operational Lifecycle

1. Package install copies app to /opt/scraping-service.
2. postinst creates service user and writable directories.
3. postinst enables and starts scraping-service.
4. UFW rule is added when firewall is active.
5. On reboot, systemd auto-starts the API.
