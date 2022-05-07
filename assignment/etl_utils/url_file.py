#class UrlFile project Unity assignment
#Author: Adrian Jimenez 2022-05

import requests

from pathlib import Path
from requests.models import Response

class UrlFile:
    """A class interface for the file to download from the url
    ---
    @url: complete string of the url file path
    @target_path: should be relevant to where the script runs i.e. '././datalake_zone/source_system/folder
    @file_name: string containing the file name and the extension
    """
    
    def __init__(self, url:str, target_path: str, file_name:str):
        self.url = url
        self.target_path = target_path
        self.file_name = file_name
        
    def get_file_from_url(self):
        """download a single file from a url, return the file
        ------
        TO-DO: raise errors for request status codes
        """
        try:
            r = requests.get(self.url)
            status_code = r.status_code
            print(f"INFO: url get request completed response code: {status_code}")
            return r
        except requests.exceptions.RequestException as e:
            print(f"INFO[ERROR]: {e}")
    
    def write_file_to_datalake(self, request:Response):
        """Method to write to datalake raw files"""
        data_lake_path = Path(self.target_path)
        data_lake_path.mkdir(parents=True, exist_ok=True)
        open(data_lake_path / self.file_name, 'wb').write(request.content)
        print(f"INFO: file {self.file_name} written on path {data_lake_path}")
        
    def add_folder_partition(self):
        """TO-DO: do something cool here for to add folder partition to the data lake for better management. 
        example: dinamically add landinzone/source/file_name/yyyy-mm/yyyy-mm-dd/file.extention 
        """