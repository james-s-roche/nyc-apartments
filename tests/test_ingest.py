import os
import types
import builtins
import pytest
from unittest.mock import patch, MagicMock

# Target module
from scraping import ingest_listings as ingest


@pytest.fixture(autouse=True)
def set_env_defaults(monkeypatch):
    # Ensure DEFAULT_PAGES if not provided
    monkeypatch.setenv("DEFAULT_PAGES", "2")
    # Generic settings required by dependencies
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "3306")
    monkeypatch.setenv("DB_NAME", "testdb")
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.setenv("DB_PASSWORD", "pass")
    # Scrape config defaults
    monkeypatch.setenv("SCRAPE_REQUEST_DELAY_SECONDS", "0.0")
    monkeypatch.setenv("SCRAPE_REQUEST_TIMEOUT_SECONDS", "1")


class DummyPreview:
    def __init__(self, i, **overrides):
        self.external_id = f"ext-{i}"
        self.url = f"http://example.com/{i}"
        self.address = f"Addr {i}"
        self.neighborhood = "Midtown"
        self.borough = "Manhattan"
        self.beds = 1
        self.baths = 1
        self.sqft = None
        self.price = 3000
        self.fee = False
        self.latitude = None
        self.longitude = None
        # Extended API fields used by ingest
        self.area_name = self.neighborhood
        self.available_at = None
        self.building_type = "RENTAL"
        self.full_bathroom_count = 1
        self.half_bathroom_count = 0
        self.furnished = False
        self.has_tour_3d = False
        self.has_videos = False
        self.interesting_price_delta = None
        self.is_new_development = False
        self.lease_term = None
        self.living_area_size = None
        self.months_free = 0
        self.net_effective_price = 0
        self.off_market_at = None
        self.rello_express = None
        self.slug = None
        self.source_group_label = "Test Broker"
        self.source_type = "PARTNER"
        self.state = "NY"
        self.status = "ACTIVE"
        self.street = self.address
        self.upcoming_open_house = None
        self.display_unit = None
        self.url_path = f"/unit/{i}"
        self.zip_code = "10019"
        self.tier = None
        self.typename = "TestEdge"
        for k, v in overrides.items():
            setattr(self, k, v)
        # Keep living_area_size in sync with sqft override when provided
        if self.sqft is not None and self.living_area_size is None:
            self.living_area_size = self.sqft


def _invoke_click(cmd, *args):
    # Helper to invoke click command function directly
    # We bypass click runner to keep dependencies minimal; call the underlying function.
    return cmd.callback(*args)


def test_ingest_ingests_multiple_pages_and_commits(monkeypatch):
    # Arrange: mock scraper to return two pages then empty
    previews_page1 = [DummyPreview(1), DummyPreview(2)]
    previews_page2 = [DummyPreview(3)]

    mock_scraper = MagicMock()
    mock_scraper.search_rentals.side_effect = [previews_page1, previews_page2, []]

    # Patch StreetEasyScraper to return our mock
    with patch("scraping.ingest_listings.StreetEasyScraper", return_value=mock_scraper):
        # Capture inserted rows via DB mock
        upserts = []
        class DummyDB:
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                return False
            def upsert_listing(self, listing):
                upserts.append(listing)
                return len(upserts)
        with patch("scraping.ingest_listings.MySQLClient", return_value=DummyDB()):
            # Act
            ingest.main.callback(
                neighborhood="midtown",
                beds=1,
                max_price=4000,
                pages=3,
                delay=0.0,
                timeout=1,
                details=False,
            )

    # Assert
    assert len(upserts) == 3
    # Validate the constructed Listing has expected mapping
    assert upserts[0].source == "streeteasy"
    assert upserts[0].external_id == "ext-1"
    assert upserts[0].price == 3000


def test_ingest_stops_on_empty_previews(monkeypatch):
    mock_scraper = MagicMock()
    mock_scraper.search_rentals.side_effect = [[]]  # first page empty -> stop immediately
    with patch("scraping.ingest_listings.StreetEasyScraper", return_value=mock_scraper):
        upserts = []
        class DummyDB:
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                return False
            def upsert_listing(self, listing):
                upserts.append(listing)
                return 1
        with patch("scraping.ingest_listings.MySQLClient", return_value=DummyDB()):
            ingest.main.callback(
                neighborhood="midtown",
                beds=0,
                max_price=2500,
                pages=5,
                delay=0.0,
                timeout=1,
                details=False,
            )
    assert len(upserts) == 0


def test_ingest_uses_env_default_pages_when_none(monkeypatch):
    # Ensure DEFAULT_PAGES is 2 per fixture; we expect exactly 2 page calls
    mock_scraper = MagicMock()
    # two non-empty pages then empty to ensure loop would break if exceeded
    p1 = [DummyPreview(1)]
    p2 = [DummyPreview(2)]
    p3 = []
    mock_scraper.search_rentals.side_effect = [p1, p2, p3]

    with patch("scraping.ingest_listings.StreetEasyScraper", return_value=mock_scraper):
        class DummyDB:
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                return False
            def upsert_listing(self, listing):
                return 1
        with patch("scraping.ingest_listings.MySQLClient", return_value=DummyDB()):
            ingest.main.callback(
                neighborhood="midtown",
                beds=1,
                max_price=4000,
                pages=None,
                delay=0.0,
                timeout=1,
                details=False,
            )

    # Assert: called exactly for pages 1..2 (DEFAULT_PAGES=2)
    calls = mock_scraper.search_rentals.call_args_list
    assert len(calls) == 2
    for idx, call in enumerate(calls, start=1):
        kwargs = call.kwargs
        assert kwargs["page"] == idx


def test_ingest_preserves_optional_fields_with_getattr(monkeypatch):
    # Provide preview missing some attributes to ensure getattr fallback works
    previews = [
        DummyPreview(1, sqft=550, latitude=40.0),
        DummyPreview(2)  # defaults include None for many optional fields
    ]
    mock_scraper = MagicMock()
    mock_scraper.search_rentals.side_effect = [previews, []]

    captured = []
    class DummyDB:
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False
        def upsert_listing(self, listing):
            captured.append(listing)
            return len(captured)

    with patch("scraping.ingest_listings.StreetEasyScraper", return_value=mock_scraper), \
         patch("scraping.ingest_listings.MySQLClient", return_value=DummyDB()):
        ingest.main.callback(
            neighborhood="williamsburg",
            beds=1,
            max_price=4500,
            pages=1,
            delay=0.0,
            timeout=1,
            details=False,
        )

    assert captured[0].sqft == 550
    assert captured[0].latitude == 40.0
    assert captured[1].sqft is None
    assert captured[1].latitude is None


def test_ingest_passes_correct_filters_to_scraper(monkeypatch):
    mock_scraper = MagicMock()
    mock_scraper.search_rentals.side_effect = [[DummyPreview(1)], []]

    with patch("scraping.ingest_listings.StreetEasyScraper", return_value=mock_scraper):
        class DummyDB:
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                return False
            def upsert_listing(self, listing):
                return 1
        with patch("scraping.ingest_listings.MySQLClient", return_value=DummyDB()):
            ingest.main.callback(
                neighborhood="upper-west-side",
                beds=2,
                max_price=5000,
                pages=1,
                delay=0.0,
                timeout=1,
                details=False,
            )

    # Verify called with expected args
    mock_scraper.search_rentals.assert_any_call(
        neighborhood="upper-west-side", beds=2, max_price=5000, page=1
    )