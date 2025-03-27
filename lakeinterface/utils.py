from io import BytesIO
import zipfile


def open_zip_archive(zip_obj):
    buffer = BytesIO(zip_obj.read())
    z = zipfile.ZipFile(buffer)
    
    return {'file_names': z.namelist(), 'zip_content': z}
    