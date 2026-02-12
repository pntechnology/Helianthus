# Helianthus 🌻

Helianthus is a data-driven platform for cataloging paintings and their museum locations using structured ingestion from Wikidata.

The project is designed to:

Normalize artist and painting data

Track current and historical painting locations

Provide a clean, extensible API

Serve as a foundation for future user-based tracking of viewed works

# Why Helianthus?

Helianthus (the genus name for sunflowers) nods to Vincent van Gogh’s iconic Sunflowers series while remaining artist-agnostic and extensible.

The goal is to build a clean, normalized cultural dataset that supports:

Multi-artist support

Museum location tracking

Future user-based painting tracking

API-first design

## Architecture (Planned)

Python + FastAPI

PostgreSQL

SQLAlchemy ORM

Alembic migrations

Wikidata ingestion layer

Docker-based local development

## Roadmap

 Define relational schema

 Implement Wikidata ingestion

 Expose read-only API endpoints

 Normalize location handling

 Add user tracking (future phase)