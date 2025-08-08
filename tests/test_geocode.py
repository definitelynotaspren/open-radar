from radar.geocode import GeoCoder


class DummyLocation:
    def __init__(self, lat, lon):
        self.latitude = lat
        self.longitude = lon
        self.raw = {"importance": 0.5}


def test_geocode_cache(tmp_path, monkeypatch):
    gc = GeoCoder(str(tmp_path / "cache.sqlite"), user_agent="test")
    monkeypatch.setattr(gc.geocoder, "geocode", lambda q: DummyLocation(1.0, 2.0))
    lat, lon, acc = gc.geocode("Somewhere")
    assert (lat, lon) == (1.0, 2.0)
    called = {"count": 0}

    def fail(q):
        called["count"] += 1
        raise AssertionError

    monkeypatch.setattr(gc.geocoder, "geocode", fail)
    lat2, lon2, _ = gc.geocode("Somewhere")
    assert (lat2, lon2) == (1.0, 2.0)
    assert called["count"] == 0
