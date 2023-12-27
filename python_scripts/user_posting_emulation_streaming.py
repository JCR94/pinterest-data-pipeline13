import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text

# minor custom change
# the line `sqlalchemy.create_engine(f"mysql+pymysql:...` was throwing an error that no module `pymysql` was found.
import pymysql

# needed for json serialiser for date
from datetime import date, datetime

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4", pool_pre_ping=True)
        return engine


new_connector = AWSDBConnector()

# loads one record
def load_single_datapoint(engine_connection, table_name, row_number):
    query = text(f"SELECT * FROM {table_name} LIMIT {row_number}, 1")
    selected_row = engine_connection.execute(query)
    
    for row in selected_row:
        result = dict(row._mapping)
    return result

# send one record to one stream
def put_record(record, stream_name):
    # invoke url for one record, for more records replace `record` with `records` in the invoke_url
    invoke_url = f"https://oemk7dh62m.execute-api.us-east-1.amazonaws.com/test-stage/streams/{stream_name}/record"

    payload = json.dumps({
        "StreamName": stream_name,
        "Data": record,
        "PartitionKey": "partition=0" # maybe not necessary to match partition name
    }, default=json_serial)
    
    headers = {'Content-Type': 'application/json'}

    response = requests.request("PUT", invoke_url, headers=headers, data=payload)
    return response

def run_infinite_post_data_loop():
    n = 1
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            
            print(f"Entry {n} incoming ...")
            pin_result = load_single_datapoint(connection, 'pinterest_data', random_row)
            geo_result = load_single_datapoint(connection, 'geolocation_data', random_row)
            user_result = load_single_datapoint(connection, 'user_data', random_row)

            response_pin = put_record(pin_result, 'streaming-0a6a638f5991-pin')
            response_geo = put_record(geo_result, 'streaming-0a6a638f5991-geo')
            response_user = put_record(user_result, 'streaming-0a6a638f5991-user')

            print(response_pin.status_code)
            print(response_geo.status_code)
            print(response_user.status_code)
        n+=1

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')