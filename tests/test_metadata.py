import random

from lakeinterface.datalake import Datalake

def test_save_with_metadata():
    lake = Datalake('example')
    location = 'pytest/test_put_metadata'
    
    content =[
        {'col1': 1, 'col2': 2},
        {'col1': 3, 'col2': 4}
    ]

    metadata = {
        'foo': 'bar'
    }

    lake.put(location, content, metadata=metadata)
    test_metadata = lake.get_metadata(location)

    assert test_metadata.get('foo') == 'bar'


def test_update_metadata():

    lake = Datalake('example')
    location = 'pytest/test_update_metadata'

    content =[
        {'col1': 1, 'col2': 2},
        {'col1': 3, 'col2': 4}
    ]

    metadata = {
        'foo': 'bar'
    }

    lake.put(location, content, metadata=metadata)

    n = str(random.randint(0,1000))
    lake.update_metadata(location, {'randint': n})

    test_metadata = lake.get_metadata(location)
    assert test_metadata == {'foo': 'bar', 'randint': n}


def test_update_empty_metadata():

    lake = Datalake('example')
    location = 'pytest/test_update_empty_metadata'

    content =[
        {'col1': 1, 'col2': 2},
        {'col1': 3, 'col2': 4}
    ]

    lake.put(location, content)

    n = str(random.randint(0,1000))
    lake.update_metadata(location, {'randint': n})

    test_metadata = lake.get_metadata(location)
    assert test_metadata == {'randint': n}

def test_overwrite_metadata():

    lake = Datalake('example')
    location = 'pytest/test_overwrite_metadata'

    content =[
        {'col1': 1, 'col2': 2},
        {'col1': 3, 'col2': 4}
    ]

    metadata = {
        'foo': 'bar'
    }

    lake.put(location, content, metadata=metadata)

    n = str(random.randint(0,1000))
    lake.update_metadata(location, {'randint': n}, overwrite=True)

    test_metadata = lake.get_metadata(location)
    assert test_metadata == {'randint': n}
