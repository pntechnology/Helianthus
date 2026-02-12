import os
import requests
from urllib.parse import urlparse
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///helianthus.db")

Base = declarative_base()


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
	paintings = relationship("Painting", back_populates="location")


class Painting(Base):
	__tablename__ = "paintings"
	id = Column(Integer, primary_key=True)
	wikidata_id = Column(String, unique=True, index=True, nullable=False)
	title = Column(String, nullable=True)
	year = Column(String, nullable=True)
	artist_id = Column(Integer, ForeignKey("artists.id"), nullable=True)
	location_id = Column(Integer, ForeignKey("locations.id"), nullable=True)
	artist = relationship("Artist", back_populates="paintings")
	location = relationship("Location", back_populates="paintings")


def qid_from_uri(uri: str) -> str:
	return uri.rsplit("/", 1)[-1]


def ensure_session(database_url: str):
	engine = create_engine(database_url, connect_args={"check_same_thread": False} if database_url.startswith("sqlite") else {})
	Base.metadata.create_all(engine)
	Session = sessionmaker(bind=engine)
	return Session()


def run_ingest(limit: int = 200):
	session = ensure_session(DATABASE_URL)

	sparql = """
	SELECT ?painting ?paintingLabel ?creator ?creatorLabel ?location ?locationLabel ?date WHERE {
	  ?painting wdt:P31 wd:Q3305213.
	  ?painting wdt:P170 ?creator.
	  OPTIONAL { ?painting wdt:P276|wdt:P195 ?location. }
	  OPTIONAL { ?painting wdt:P571 ?date. }
	  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
	}
	LIMIT %d
	""" % limit

	url = "https://query.wikidata.org/sparql"
	headers = {"Accept": "application/sparql-results+json", "User-Agent": "HelianthusIngest/1.0 (https://github.com)"}
	r = requests.get(url, params={"query": sparql}, headers=headers, timeout=60)
	r.raise_for_status()
	data = r.json()
	bindings = data.get("results", {}).get("bindings", [])

	inserted = 0
	for row in bindings:
		p_uri = row.get("painting", {}).get("value")
		p_label = row.get("paintingLabel", {}).get("value")
		creator_uri = row.get("creator", {}).get("value")
		creator_label = row.get("creatorLabel", {}).get("value")
		location_uri = row.get("location", {}).get("value")
		location_label = row.get("locationLabel", {}).get("value")
		date_val = row.get("date", {}).get("value")

		if not p_uri or not creator_uri:
			continue

		p_qid = qid_from_uri(p_uri)
		creator_qid = qid_from_uri(creator_uri)
		location_qid = qid_from_uri(location_uri) if location_uri else None

		artist = session.query(Artist).filter_by(wikidata_id=creator_qid).first()
		if not artist:
			artist = Artist(wikidata_id=creator_qid, name=creator_label)
			session.add(artist)
			session.flush()

		location = None
		if location_qid:
			location = session.query(Location).filter_by(wikidata_id=location_qid).first()
			if not location:
				location = Location(wikidata_id=location_qid, name=location_label)
				session.add(location)
				session.flush()

		painting = session.query(Painting).filter_by(wikidata_id=p_qid).first()
		if not painting:
			painting = Painting(wikidata_id=p_qid, title=p_label, year=date_val if date_val else None, artist=artist, location=location)
			session.add(painting)
			inserted += 1
		else:
			# update fields if missing
			painting.title = painting.title or p_label
			painting.year = painting.year or date_val
			painting.artist = painting.artist or artist
			painting.location = painting.location or location

	session.commit()
	print(f"Ingest complete — inserted/updated: {inserted} new paintings (limit={limit}).")


if __name__ == "__main__":
	import argparse

	parser = argparse.ArgumentParser(description="Ingest paintings from Wikidata into local DB")
	parser.add_argument("--limit", type=int, default=200)
	args = parser.parse_args()
	run_ingest(args.limit)
