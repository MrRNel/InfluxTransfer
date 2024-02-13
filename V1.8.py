import math
import time
import pytz
import logging
import State
from influxdb import InfluxDBClient as InfluxDBClientV1
from influxdb_client import  InfluxDBClient as InfluxDBClientV2 , OrganizationsApi, BucketsApi,Point, WriteApi
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.exceptions import ReadTimeoutError
from pprint import pprint
from project_secrets import INFLUXDB_V1_HOST, INFLUXDB_V1_PORT, INFLUXDB_V1_DATABASE, INFLUXDB_V2_URL, INFLUXDB_V2_TOKEN, INFLUXDB_V2_ORG

now = datetime.now()
# Initialize the Logger
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(f"migration_log{now.strftime('%Y-%m-%d')}.log"),  # Log messages to this file
                        logging.StreamHandler()  # Keep logging to the console as well
                    ])

# Configuration for InfluxDB v1.8
INFLUXDB_V1_HOST = INFLUXDB_V1_HOST
INFLUXDB_V1_PORT = INFLUXDB_V1_PORT
INFLUXDB_V1_DATABASE = INFLUXDB_V1_DATABASE

# Configuration for InfluxDB v2.7
INFLUXDB_V2_URL = INFLUXDB_V2_URL
INFLUXDB_V2_TOKEN = INFLUXDB_V2_TOKEN
INFLUXDB_V2_ORG = INFLUXDB_V2_ORG

# Connect to InfluxDB v1.8
client_v1 = InfluxDBClientV1(host=INFLUXDB_V1_HOST, port=INFLUXDB_V1_PORT)
client_v1.switch_database(INFLUXDB_V1_DATABASE)

# Connect to InfluxDB v2.7
client_v2 = InfluxDBClientV2(url=INFLUXDB_V2_URL, token=INFLUXDB_V2_TOKEN, org=INFLUXDB_V2_ORG)
write_api = client_v2.write_api(write_options=SYNCHRONOUS)

def get_or_create_bucket(bucket_name):
    buckets_api = client_v2.buckets_api()
    orgs_api = client_v2.organizations_api()
    org = orgs_api.find_organizations(org_id=INFLUXDB_V2_ORG)[0]

    existing_buckets = buckets_api.find_bucket_by_name(bucket_name)
    if existing_buckets:
        return existing_buckets
    else:
        return buckets_api.create_bucket(bucket_name=bucket_name, org_id=org.id)

def get_time_range_for_measurement(measurement):
    earliest_time_query = client_v1.query(f'SELECT * FROM "{measurement}" ORDER BY time ASC LIMIT 1')
    latest_time_query = client_v1.query(f'SELECT * FROM "{measurement}" ORDER BY time DESC LIMIT 1')
    earliest_time = list(earliest_time_query.get_points())[0]['time']
    latest_time = list(latest_time_query.get_points())[0]['time']
    return earliest_time, latest_time

def write_batch(measurement, start_dt, end_dt, bucket_name, time_format, retry_attempts=5, retry_delay=2):    
    query = f'SELECT * FROM "{measurement}" WHERE time >= \'{start_dt.strftime(time_format)}\' AND time < \'{end_dt.strftime(time_format)}\''
    #query = f'SELECT * FROM "{measurement}" ORDER BY time ASC LIMIT 1'    
    results = client_v1.query(query)
    
    points = []
    for point in results.get_points():
        try:
            point_time = datetime.strptime(point['time'], '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            point_time = datetime.strptime(point['time'], '%Y-%m-%dT%H:%M:%S%z')
        new_point = Point(measurement).time(point_time)
        for key, value in point.items():
            if key != 'time':
                # Check if the value is a boolean or non-numeric and handle accordingly
                if isinstance(value, bool):
                    new_point.field(key, value)
                elif isinstance(value, (int, float)):
                    new_point.field(key, value)
                else:
                    new_point.tag(key, str(value))  # Convert non-numeric values to strings for tags
        points.append(new_point)
    
    for attempt in range(1, retry_attempts + 1):
        try:
            if points:                
                 write_api.write(bucket=bucket_name, org=INFLUXDB_V2_ORG, record=points)                 
                 #logging.info(f"Completed data transfer for {measurement} on {start_dt.strftime('%Y-%m-%dT%H:%M:%S')}.") # Enable this only for debugging 
            break  # Exit the loop if write succeeds
        except ReadTimeoutError:
            if attempt == retry_attempts:
                State.add_writeobj(measurement, start_dt, end_dt, bucket_name, time_format)
                logging.error(f"Failed to write data for {measurement} on {start_dt.strftime('%Y-%m-%d %H:%M:%S')} after {retry_attempts} attempts.")
                raise  # Reraise the exception if the last attempt fails
            else:
                logging.info(f"Retry attempt {attempt} for {measurement} entry: {start_dt.strftime('%Y-%m-%d %H:%M:%S')} after delay.")
                time.sleep(retry_delay)  # Wait before retrying

def batch_write_data(measurement, start_time, end_time, bucket_name, concurrency=5):
    start_dt, start_format = parse_datetime(start_time)
    end_dt, _ = parse_datetime(end_time)
    delta = timedelta(minutes=3) if measurement == 'Single' else timedelta(minutes=5)  # Changed to 6-hour increments time out at 30 minutes

    futures = []
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        while start_dt < end_dt:
            next_dt = min(start_dt + delta, end_dt)  # Ensure we do not go beyond end_dt
            #logging.info(f"Processing data for {measurement} on {start_dt.strftime('%Y-%m-%dT%H:%M:%S')}.") # Enable this only when debuging 
            future = executor.submit(write_batch, measurement, start_dt, next_dt, bucket_name, start_format)
            #future = executor.submit(test_write_top_10_data(measurement,bucket_name))
            futures.append(future)
            start_dt = next_dt        
        for future in as_completed(futures):
            try:
                future.result()  # This will re-raise any exceptions caught in the thread
            except Exception as e:
                State.add_writeobj(measurement, start_dt, end_dt, bucket_name, start_format)
                logging.error(f"An error occurred: {e}")


def get_last_entry_date_from_v2(bucket_name):
    query_api = client_v2.query_api()
    query = f'from(bucket:"{bucket_name}") |> range(start: -1y) |> last()'
    results = query_api.query(org=INFLUXDB_V2_ORG, query=query)
    if results and len(results) > 0 and len(results[0].records) > 0:
        return results[0].records[0].get_time()
    return None

def parse_datetime(datetime_str):
    format_with_fractional_seconds = '%Y-%m-%dT%H:%M:%S.%fZ'
    format_without_fractional_seconds = '%Y-%m-%dT%H:%M:%SZ'
    
    try:
        # Try parsing with fractional seconds
        parsed_datetime = datetime.strptime(datetime_str, format_with_fractional_seconds)
        used_format = format_with_fractional_seconds
    except ValueError:
        # If that fails, try without fractional seconds
        parsed_datetime = datetime.strptime(datetime_str, format_without_fractional_seconds)
        used_format = format_without_fractional_seconds
    
    return parsed_datetime, used_format

measurements = client_v1.query('SHOW MEASUREMENTS').get_points()
measurement_names = [measurement['name'] for measurement in measurements]

if State.writeObjs:
    while State.writeObjs:
        obj = State.pop_writeobj()
        bucket = get_or_create_bucket(obj.measurement)
        batch_write_data(obj.measurement, obj.start_dt, obj.end_dt, bucket.name, concurrency=1)       

for measurement in measurement_names:
    logging.info(f"Starting migration for measurement: {measurement}")

    bucket = get_or_create_bucket(measurement)
        
    last_entry_date_v2 = get_last_entry_date_from_v2(bucket.name)
        
    if last_entry_date_v2:
        start_time_v2 = last_entry_date_v2.strftime('%Y-%m-%dT%H:%M:%SZ')
        start_time = start_time_v2
    else:
    
        start_time_v1, _ = get_time_range_for_measurement(measurement)
        start_time = start_time_v1
    

    _, end_time = get_time_range_for_measurement(measurement)
    
    batch_write_data(measurement, start_time, end_time, bucket.name, concurrency=5)
    logging.info(f"Completed migration for measurement: {measurement}")


# for measurement in measurement_names:
#     bucket = get_or_create_bucket(measurement)
#     earliest_time, latest_time = get_time_range_for_measurement(measurement)
#     batch_write_data(measurement, earliest_time, latest_time, bucket.name, concurrency=5)
#      # Use the test function for writing top 10 data points
#     #test_write_top_10_data(measurement, bucket.name)

