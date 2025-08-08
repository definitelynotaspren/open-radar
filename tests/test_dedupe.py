from radar import dedupe


def test_is_dupe():
    h = dedupe.simhash_of("test")
    assert not dedupe.is_dupe(h)
    assert dedupe.is_dupe(h)
