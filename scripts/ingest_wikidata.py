import os
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


DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///helianthus.db")
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"

Base = declarative_base()


# =========================
# Models
# =========================

class Artist(Base):
    __tablename__ = "artists"

    id = Column(Integer, primary_key=True)
    wikidata_id = Column(String, unique=True, index=True, nullable=False)
    name = Column(String, nullable=True)

    paintings = relationship("Painting", back_populates="artist")


class Location(Base):
    __tablename__ = "locations"

    id = Column(Integer, primary_key=True)
    wikidata_id = Column(String, unique=True, index=True, nullable=True)
    name = Column(String, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)

    paintings = relationship("Painting", back_populates="location")


class Painting(Base):
    __tablename__ = "paintings"

    id = Column(Integer, primary_key=True)
    wikidata_id = Column(String, unique=True, index=True, nullable=False)
    title = Column(String, nullable=True)
    year = Column(Integer, nullable=True)

    artist_id = Column(Integer, ForeignKey("artists.id"), nullable=False)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=True)

    artist = relationship("Artist", back_populates="paintings")
    location = relationship("Location", back_populates="paintings")


# =========================
# Helpers
# =========================

def qid_from_uri(uri: str) -> str:
    return uri.rsplit("/", 1)[-1]


def ensure_session(database_url: str):
    engine = create_engine(
        database_url,
        connect_args={"check_same_thread": False}
        if database_url.startswith("sqlite")
        else {}
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def validate_artist_is_painter(artist_qid: str) -> bool:
    """
    Ensures the QID belongs to a painter (occupation = painter).
    """
    query = f"""
    ASK {{
      wd:{artist_qid} wdt:P106 wd:Q1028181.
    }}
    """

    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "HelianthusIngest/1.0 (https://github.com)"
    }

    r = requests.get(
        WIKIDATA_ENDPOINT,
        params={"query": query},
        headers=headers,
        timeout=30
    )
    r.raise_for_status()
    return r.json().get("boolean", False)


# =========================
# Ingest Logic
# =========================

def run_ingest(artist_qid: str, limit: int = 200):

    if not validate_artist_is_painter(artist_qid):
        raise ValueError(...)

    session = ensure_session(DATABASE_URL)

    sparql = f"""
    SELECT ?painting ?paintingLabel WHERE {{
      ?painting wdt:P31 wd:Q3305213.
      ?painting wdt:P170 wd:{artist_qid}.
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT {limit}
    """

    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "HelianthusIngest/1.0 (https://github.com)"
    }

    r = requests.get(
        WIKIDATA_ENDPOINT,
        params={"query": sparql},
        headers=headers,
        timeout=60
    )

    r.raise_for_status()

    bindings = r.json().get("results", {}).get("bindings", [])

    inserted = 0

    for row in bindings:
        p_uri = row.get("painting", {}).get("value")
        p_label = row.get("paintingLabel", {}).get("value")
        creator_label = row.get("creatorLabel", {}).get("value")
        location_uri = row.get("location", {}).get("value")
        location_label = row.get("locationLabel", {}).get("value")
        coords_val = row.get("coords", {}).get("value")
        date_val = row.get("date", {}).get("value")

        if not p_uri:
            continue

        p_qid = qid_from_uri(p_uri)

        # -----------------------
        # Year parsing
        # -----------------------
        year = None
        if date_val:
            try:
                year = datetime.fromisoformat(
                    date_val.replace("Z", "")
                ).year
            except Exception:
                pass

        # -----------------------
        # Coordinates parsing
        # -----------------------
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

        # -----------------------
        # Artist
        # -----------------------
        artist = session.query(Artist).filter_by(
            wikidata_id=artist_qid
        ).first()

        if not artist:
            artist = Artist(
                wikidata_id=artist_qid,
                name=creator_label
            )
            session.add(artist)
            session.flush()

        # -----------------------
        # Location
        # -----------------------
        location = None

        if location_uri:
            location_qid = qid_from_uri(location_uri)

            location = session.query(Location).filter_by(
                wikidata_id=location_qid
            ).first()

            if not location:
                location = Location(
                    wikidata_id=location_qid,
                    name=location_label,
                    latitude=latitude,
                    longitude=longitude
                )
                session.add(location)
                session.flush()
            else:
                # overwrite missing coordinates
                if latitude and not location.latitude:
                    location.latitude = latitude
                if longitude and not location.longitude:
                    location.longitude = longitude

        # -----------------------
        # Painting
        # -----------------------
        painting = session.query(Painting).filter_by(
            wikidata_id=p_qid
        ).first()

        if not painting:
            painting = Painting(
                wikidata_id=p_qid,
                title=p_label,
                year=year,
                artist=artist,
                location=location
            )
            session.add(painting)
            inserted += 1
        else:
            painting.title = p_label or painting.title
            painting.year = year or painting.year
            painting.artist = artist
            painting.location = location

    session.commit()

    print(
        f"Ingest complete for {artist_qid} — "
        f"{inserted} new paintings (limit={limit})"
    )


# =========================
# CLI Entry
# =========================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Ingest paintings for a specific artist from Wikidata"
    )
    parser.add_argument(
        "--artist",
        required=True,
        help="Wikidata QID for artist (e.g., Q5582)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200
    )

    args = parser.parse_args()

    run_ingest(args.artist, args.limit)