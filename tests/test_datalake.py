import pytest
import random
import polars as pl


from lakeinterface.s3_object_factory import S3ObjectNotFound
from lakeinterface.datalake import Datalake
 

def test_missing_object_raises_exception():
    lake = Datalake('example')
    with pytest.raises(S3ObjectNotFound):
        lake.get('does/not/exist')


def test_missing_object_with_not_found_value():
    lake = Datalake('example')
    assert lake.get('does/not/exist', not_found_value='foo') == 'foo'

def test_get_object():
    lake = Datalake('example')
    df = lake.get('pytest/example1/data.parquet')
    assert df.row(0)[0] == 1

def test_put_dataframe():
    lake = Datalake('example')
    location = 'pytest/test_put_object'
    rint = random.randint(0,1000)
    d = {'col1': [rint, 2], 'col2': [3, 4]}
    df = pl.DataFrame(data=d)
    lake.put(location, df)

    df2 = lake.get(location)
    assert df2.row(0)[0] == rint


def test_put_dataframe_to_csv():
    lake = Datalake('example')
    location = 'pytest/test_csv_put_object'
    rint = random.randint(0,1000)
    d = {'col1': [rint, 2], 'col2': [3, 4]}
    df = pl.DataFrame(data=d)
    lake.put(location, df, file_type='csv')

    df2 = lake.get(location)
    assert df2.row(0)[0] == rint


def test_put_json():
    lake = Datalake('example')
    location = 'pytest/test_put_json_object'
    
    content =[
        {'col1': 1, 'col2': 2},
        {'col1': 3, 'col2': 4}
    ]
    lake.put(location, content)

    test_content = lake.get(location)
    assert test_content[0]['col1'] == 1

def test_list_objects():
    lake = Datalake('example')
    test_objects = lake.list_objects(prefix='pytest')
    assert 'pytest/example1/data.parquet' in test_objects

def test_most_recent():
    lake = Datalake('example')
    most_recent = lake.most_recent('pytest/example_with_timestamp')
    assert most_recent == 'pytest/example_with_timestamp/20991021/data.parquet'

def test_most_recent_timestamp():
    lake = Datalake('example')
    most_recent = lake.most_recent_folder('pytest/example_with_timestamp')
    assert most_recent == '20991021'
    