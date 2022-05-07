#Author: Adrian Jimenez 2022-05
import json
from pathlib import Path
from pyspark.sql.types import StructType, StructField

class TableSchema:
    """
    Class to represent a spark table schema
    @table_name: string
    @schema_location_path: path where json file of the schema will be saved
    @schema: schema taken from the df after applying the schema function
    @absolute_schema_path
    """
    def __init__(self, table_name:str, schema_root_path:str):
        self.table_name = table_name
        self.schema_root_path = schema_root_path
        self.absolute_schema_path = f'{self.schema_root_path}/schema_{table_name}.json'
        
    def save_schema_file_as_json(self, schema_in_json: json):
        path = Path(self.schema_root_path)
        path.mkdir(parents=True, exist_ok=True)
        with open(self.absolute_schema_path, 'w') as p:
            json.dump(schema_in_json, p)
          
    def load_schema_json(self):
        with open(self.absolute_schema_path, 'r') as f:
            schema_json = json.load(f)
        return StructType.fromJson(json.loads(schema_json))