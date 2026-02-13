import os
import time
import requests
from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    ForeignKey,
    create_engine,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker


# =========================
# Config
# =========================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = f"sqlite:///{os.path.join(BASE_DIR, 'helianthus.db')}"
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"

Base = declarative_base()


# =========================
# Models
# =========================

class Artist(Base):
    __tablename__ = "artists"

    id = Column(Integer, primary_key=True)
    wikidata_id = Column(String, unique=True, index=True, nullable=False)
    name = Column(String)

    paintings = relationship("Painting", back_populates="artist")


class Location(Base):
    __tablename__ = "locations"

    id = Column(Integer, primary_key=True)
    wikidata_id = Column(String, unique=True, index=True)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)

    paintings = relationship("Painting", back_populates="location")


class Painting(Base):
    __tablename__ = "paintings"

    id = Column(Integer, primary_key=True)
    wikidata_id = Column(String, unique=True, index=True, nullable=False)
    title = Column(String)
    year = Column(Integer)

    artist_id = Column(Integer, ForeignKey("artists.id"), nullable=False)
    location_id = Column(Integer, ForeignKey("locations.id"))

    artist = relationship("Artist", back_populates="paintings")
    location = relationship("Location", back_populates="paintings")


# =========================
# Helpers
# =========================

def qid_from_uri(uri: str) -> str:
    return uri.rsplit("/", 1)[-1]


def ensure_session():
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def wikidata_query(query: str, timeout=30):
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "HelianthusIngest/1.0"
    }

    for attempt in range(3):
        try:
            r = requests.get(
                WIKIDATA_ENDPOINT,
                params={"query": query},
                headers=headers,
                timeout=timeout
            )
            r.raise_for_status()
            return r.json()
        except requests.exceptions.ReadTimeout:
            print(f"Timeout attempt {attempt+1}, retrying...")
            time.sleep(3)

    raise Exception("Wikidata query failed after retries")


# =========================
# Artist Metadata
# =========================

def fetch_artist_label(artist_qid: str) -> str:
    query = f"""
    SELECT ?artistLabel WHERE {{
      wd:{artist_qid} rdfs:label ?artistLabel.
      FILTER (lang(?artistLabel) = "en")
    }}
    """

    data = wikidata_query(query)
    bindings = data.get("results", {}).get("bindings", [])

    if not bindings:
        return None

    return bindings[0]["artistLabel"]["value"]


# =========================
# Phase 1 – Paintings
# =========================

def ingest_paintings(session, artist_qid: str, limit: int):
    print("Phase 1: Ingesting paintings...")

    sparql = f"""
    SELECT ?painting ?paintingLabel ?date WHERE {{
      ?painting wdt:P31 wd:Q3305213.
      ?painting wdt:P170 wd:{artist_qid}.
      OPTIONAL {{ ?painting wdt:P571 ?date. }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT {limit}
    """

    data = wikidata_query(sparql, timeout=60)
    bindings = data.get("results", {}).get("bindings", [])

    # --- Artist ---
    artist = session.query(Artist).filter_by(
        wikidata_id=artist_qid
    ).first()

    if not artist:
        artist_name = fetch_artist_label(artist_qid)

        artist = Artist(
            wikidata_id=artist_qid,
            name=artist_name
        )

        session.add(artist)
        session.flush()

    inserted = 0

    # --- Paintings ---
    for row in bindings:
        p_uri = row.get("painting", {}).get("value")
        p_label = row.get("paintingLabel", {}).get("value")
        date_val = row.get("date", {}).get("value")

        if not p_uri:
            continue

        p_qid = qid_from_uri(p_uri)

        year = None
        if date_val:
            try:
                year = datetime.fromisoformat(
                    date_val.replace("Z", "")
                ).year
            except Exception:
                pass

        painting = session.query(Painting).filter_by(
            wikidata_id=p_qid
        ).first()

        if not painting:
            painting = Painting(
                wikidata_id=p_qid,
                title=p_label,
                year=year,
                artist=artist
            )
            session.add(painting)
            inserted += 1
        else:
            painting.title = p_label or painting.title
            painting.year = year or painting.year

    session.commit()
    print(f"Inserted {inserted} paintings.")

# =========================
# Phase 2 – Locations
# =========================

def enrich_locations(session):
    print("Phase 2: Enriching locations...")

    paintings = session.query(Painting).filter(
        Painting.location_id == None
    ).all()

    for painting in paintings:
        query = f"""
        SELECT ?location ?locationLabel ?coords WHERE {{
          wd:{painting.wikidata_id} wdt:P276|wdt:P195 ?location.
          OPTIONAL {{ ?location wdt:P625 ?coords. }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
        """

        data = wikidata_query(query)
        bindings = data.get("results", {}).get("bindings", [])

        if not bindings:
            continue

        row = bindings[0]

        loc_uri = row.get("location", {}).get("value")
        loc_label = row.get("locationLabel", {}).get("value")
        coords_val = row.get("coords", {}).get("value")

        if not loc_uri:
            continue

        loc_qid = qid_from_uri(loc_uri)

        latitude = None
        longitude = None

        if coords_val:
            try:
                coords_str = coords_val.replace("Point(", "").replace(")", "")
                lon_str, lat_str = coords_str.split()
                longitude = float(lon_str)
                latitude = float(lat_str)
            except Exception:
                pass

        location = session.query(Location).filter_by(
            wikidata_id=loc_qid
        ).first()

        if not location:
            location = Location(
                wikidata_id=loc_qid,
                name=loc_label,
                latitude=latitude,
                longitude=longitude
            )
            session.add(location)
            session.flush()

        painting.location = location
        session.commit()

        time.sleep(0.2)  # be nice to Wikidata

    print("Location enrichment complete.")


# =========================
# CLI Entry
# =========================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--artist", required=True)
    parser.add_argument("--limit", type=int, default=200)

    args = parser.parse_args()

    session = ensure_session()

    ingest_paintings(session, args.artist, args.limit)
    enrich_locations(session)

    print("Done.")