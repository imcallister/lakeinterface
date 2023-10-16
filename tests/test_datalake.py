import pytest
import random
import polars as pl


from lakeinterface.datalake import Datalake, S3ObjectNotFound


def test_missing_object_raises_exception():
    lake = Datalake('example')
    with pytest.raises(S3ObjectNotFound):
        lake.get('does/not/exist')


def test_missing_object_with_not_found_value():
    lake = Datalake('example')
    assert lake.get('does/not/exist', not_found_value='foo') == 'foo'


def test_put_object():
    lake = Datalake('example')
    location = 'pytest/test_put_object'
    rint = random.randint(0,1000)
    d = {'col1': [rint, 2], 'col2': [3, 4]}
    df = pl.DataFrame(data=d)
    lake.put(location, df)

    df2 = lake.get(location)
    assert df2.row(0)[0] == rint


def test_list_objects():
    lake = Datalake('example')
    test_objects = lake.list_objects(prefix='pytest')
    assert 'pytest/example1/data.parquet' in test_objects


def test_most_recent():
    lake = Datalake('example')
    most_recent = lake.most_recent_folder('pytest/example_with_timestamp')
    assert most_recent == '20991021'
    