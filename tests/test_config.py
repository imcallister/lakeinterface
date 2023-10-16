from lakeinterface.config import lake_config


def test_load_config():
    cfg = lake_config('example')
    assert cfg.get('param1') == 'foo'
    assert cfg.get('param2') == 'bar'
