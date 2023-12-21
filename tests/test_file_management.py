from lakeinterface.datalake import Datalake


def test_save_file():
    lake = Datalake('example')
    file_name = 'H8_struct.xml'
    destination_folder = 'pytest/saving_file'
    with open('./tests/test_objects/H8_struct.xml', 'rb') as f:
        resp = lake.save_file(f, destination_folder, file_name)

    assert resp == 200


def test_load_file():
    lake = Datalake('example')
    file_name = 'testing.txt'
    location_folder = 'pytest/loading_file'
    resp = lake.load_file(f'{location_folder}/{file_name}')
    file_content = resp.read()
    assert file_content == b'testing'