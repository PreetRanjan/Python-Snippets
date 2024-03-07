"""
Author: PREETish
Reach me at: https://www.pritishranjan.com
Queries: https://preetblogs.azurewebsites.net/aboutme
Github: PreetRanjan
"""

import zipfile
import io
from azure.storage.filedatalake import FileSystemClient

connection_string = "<your_connection_string>"
file_system_client = FileSystemClient.from_connection_string(connection_string, file_system_name="samples")

def upload_bytes_to_adls(file_system_client,file_path, file_contents):
    file_client = file_system_client.get_file_client(file_path)
    # Upload bytes to the file
    file_client.upload_data(file_contents, overwrite=True)

def read_file_from_adls(file_system_client,file_path):
    file_client = file_system_client.get_file_client(file_path)
    download = file_client.download_file()
    downloaded_bytes = download.readall()
    return downloaded_bytes

def extract_zip_in_adls(zip_data,extract_dir):
    with io.BytesIO(zip_data) as zip_buffer:
        with zipfile.ZipFile(zip_buffer, "r") as zip_file:
            for file_name in zip_file.namelist():
                with zip_file.open(file_name) as file_in_zip:
                    extract_path = extract_dir + file_name
                    print("Extract & Upload to: ",extract_path)
                    upload_bytes_to_adls(file_system_client,extract_path,file_in_zip.read())
                    print("Uploaded!!")
zip_file_path = "drivetime.zip"
print("Reading ZIP file:",zip_file_path)
zip_bytes = read_file_from_adls(file_system_client,zip_file_path)
print("Zip file Read. Size: ",len(zip_bytes)," Bytes")

# Extract ZIP files and upload each file to ADLS
print("Running Extract and Uplaod...")
extract_zip_in_adls(zip_bytes,"Extract/")
