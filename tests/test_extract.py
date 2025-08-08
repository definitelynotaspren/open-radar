from radar import extract


def test_extract_candidates():
    cands = extract.extract_candidates("Fire in London", "Fire in London")
    assert any("London" in c.text for c in cands)


def test_extract_event_time():
    dt = extract.extract_event_time("2024-01-01")
    assert dt.year == 2024


def test_classify_event_type():
    assert extract.classify_event_type("Reported burglary") == "burglary"
