from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Iterable, Optional
import mysql.connector
from mysql.connector import MySQLConnection

from config.settings import load_config


@dataclass
class Listing:
    source: str
    external_id: str
    url: str
    address: Optional[str] = None
    neighborhood: Optional[str] = None
    borough: Optional[str] = None
    beds: Optional[float] = None
    baths: Optional[float] = None
    sqft: Optional[int] = None
    price: Optional[int] = None
    fee: Optional[bool] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    building_name: Optional[str] = None
    unit: Optional[str] = None
    pets: Optional[str] = None
    amenities: Optional[str] = None
    broker: Optional[str] = None
    listed_at: Optional[str] = None
    # New fields mirroring db.schema listingData.node
    area_name: Optional[str] = None
    available_at: Optional[str] = None
    building_type: Optional[str] = None
    full_bathroom_count: Optional[int] = None
    half_bathroom_count: Optional[int] = None
    furnished: Optional[bool] = None
    has_tour_3d: Optional[bool] = None
    has_videos: Optional[bool] = None
    interesting_price_delta: Optional[int] = None
    is_new_development: Optional[bool] = None
    lease_term: Optional[int] = None
    living_area_size: Optional[int] = None
    months_free: Optional[int] = None
    net_effective_price: Optional[int] = None
    off_market_at: Optional[str] = None
    rello_express: Optional[str] = None
    slug: Optional[str] = None
    source_group_label: Optional[str] = None
    source_type: Optional[str] = None
    state: Optional[str] = None
    status: Optional[str] = None
    street: Optional[str] = None
    upcoming_open_house: Optional[str] = None
    display_unit: Optional[str] = None
    url_path: Optional[str] = None
    zip_code: Optional[str] = None
    tier: Optional[str] = None


class MySQLClient:
    def __init__(self):
        cfg = load_config()
        self.conn: MySQLConnection = mysql.connector.connect(
            host=cfg.db.host,
            port=cfg.db.port,
            database=cfg.db.name,
            user=cfg.db.user,
            password=cfg.db.password,
        )
        self.conn.autocommit = False

    def close(self):
        if self.conn.is_connected():
            self.conn.close()

    def upsert_listing(self, listing: Listing) -> int:
        sql = (
            "INSERT INTO listings (source, external_id, url, address, neighborhood, borough, beds, baths, sqft, price, fee, latitude, longitude, building_name, unit, pets, amenities, broker, listed_at, "
            "area_name, available_at, building_type, full_bathroom_count, half_bathroom_count, furnished, has_tour_3d, has_videos, interesting_price_delta, is_new_development, lease_term, living_area_size, months_free, net_effective_price, off_market_at, rello_express, slug, source_group_label, source_type, state, status, street, upcoming_open_house, display_unit, url_path, zip_code, tier) "
            "VALUES (%(source)s, %(external_id)s, %(url)s, %(address)s, %(neighborhood)s, %(borough)s, %(beds)s, %(baths)s, %(sqft)s, %(price)s, %(fee)s, %(latitude)s, %(longitude)s, %(building_name)s, %(unit)s, %(pets)s, %(amenities)s, %(broker)s, %(listed_at)s, "
            "%(area_name)s, %(available_at)s, %(building_type)s, %(full_bathroom_count)s, %(half_bathroom_count)s, %(furnished)s, %(has_tour_3d)s, %(has_videos)s, %(interesting_price_delta)s, %(is_new_development)s, %(lease_term)s, %(living_area_size)s, %(months_free)s, %(net_effective_price)s, %(off_market_at)s, %(rello_express)s, %(slug)s, %(source_group_label)s, %(source_type)s, %(state)s, %(status)s, %(street)s, %(upcoming_open_house)s, %(display_unit)s, %(url_path)s, %(zip_code)s, %(tier)s) "
            "ON DUPLICATE KEY UPDATE url=VALUES(url), address=VALUES(address), neighborhood=VALUES(neighborhood), borough=VALUES(borough), beds=VALUES(beds), baths=VALUES(baths), sqft=VALUES(sqft), price=VALUES(price), fee=VALUES(fee), latitude=VALUES(latitude), longitude=VALUES(longitude), building_name=VALUES(building_name), unit=VALUES(unit), pets=VALUES(pets), amenities=VALUES(amenities), broker=VALUES(broker), listed_at=VALUES(listed_at), "
            "area_name=VALUES(area_name), available_at=VALUES(available_at), building_type=VALUES(building_type), full_bathroom_count=VALUES(full_bathroom_count), half_bathroom_count=VALUES(half_bathroom_count), furnished=VALUES(furnished), has_tour_3d=VALUES(has_tour_3d), has_videos=VALUES(has_videos), interesting_price_delta=VALUES(interesting_price_delta), is_new_development=VALUES(is_new_development), lease_term=VALUES(lease_term), living_area_size=VALUES(living_area_size), months_free=VALUES(months_free), net_effective_price=VALUES(net_effective_price), off_market_at=VALUES(off_market_at), rello_express=VALUES(rello_express), slug=VALUES(slug), source_group_label=VALUES(source_group_label), source_type=VALUES(source_type), state=VALUES(state), status=VALUES(status), street=VALUES(street), upcoming_open_house=VALUES(upcoming_open_house), display_unit=VALUES(display_unit), url_path=VALUES(url_path), zip_code=VALUES(zip_code), tier=VALUES(tier)"
        )
        with self.conn.cursor() as cur:
            cur.execute(sql, vars(listing))
            listing_id = cur.lastrowid
            if listing_id == 0:
                # Fetch existing id based on natural key (source, external_id)
                cur.execute(
                    "SELECT id FROM listings WHERE source=%s AND external_id=%s",
                    (listing.source, listing.external_id),
                )
                row = cur.fetchone()
                if row:
                    listing_id = int(row[0])
            if listing.price is not None:
                cur.execute(
                    "INSERT INTO price_history (listing_id, price) VALUES (%s, %s)",
                    (listing_id, listing.price),
                )
        return listing_id

    def commit(self):
        self.conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.close()
