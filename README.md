# 4chan-thread-data-scraper

Simple containerized Flask app.

Listens to GCP Pub/Sub message data to scrape specified 4chan boards.

The scraped thread data is then stream inserted into BigQuery.

