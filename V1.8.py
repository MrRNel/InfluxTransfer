import time
from influxdb import InfluxDBClient as InfluxDBClientV1
from influxdb_client import  InfluxDBClient as InfluxDBClientV2 , OrganizationsApi, BucketsApi,Point, WriteApi
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.exceptions import ReadTimeoutError
from pprint import pprint

from secrets import INFLUXDB_V1_HOST, INFLUXDB_V1_PORT, INFLUXDB_V1_DATABASE, INFLUXDB_V2_URL, INFLUXDB_V2_TOKEN, INFLUXDB_V2_ORG




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

def write_batch(measurement, start_dt, end_dt, bucket_name, time_format, retry_attempts=3, retry_delay=2):
    print(f"{start_dt.strftime('%Y-%m-%d %H:%M:%S')} transfer has started.")  # Print start message
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
                for point in points:                    
                    write_api.write(bucket=bucket_name, org=INFLUXDB_V2_ORG, record=point)
            print(f"{start_dt.strftime('%Y-%m-%d %H:%M:%S')} to {end_dt.strftime('%Y-%m-%d %H:%M:%S')} transfer is completed.")
            break  # Exit the loop if write succeeds
        except ReadTimeoutError:
            if attempt == retry_attempts:
                print(f"Failed to write data for {start_dt.strftime('%Y-%m-%d %H:%M:%S')} after {retry_attempts} attempts.")
                raise  # Reraise the exception if the last attempt fails
            else:
                print(f"Attempt {attempt} failed for {start_dt.strftime('%Y-%m-%d %H:%M:%S')}, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)  # Wait before retrying

def batch_write_data(measurement, start_time, end_time, bucket_name, concurrency=5):
    time_format = '%Y-%m-%dT%H:%M:%S.%fZ'
    start_dt = datetime.strptime(start_time, time_format)
    end_dt = datetime.strptime(end_time, time_format)
    delta = timedelta(minutes=5)  # Changed to 6-hour increments time out at 30 minutes

    total_duration = end_dt - start_dt
    total_periods = total_duration.total_seconds() / (6 * 60 * 60)
    futures = []
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        while start_dt < end_dt:
            next_dt = min(start_dt + delta, end_dt)  # Ensure we do not go beyond end_dt
            future = executor.submit(write_batch, measurement, start_dt, next_dt, bucket_name, time_format)
            #future = executor.submit(test_write_top_10_data(measurement,bucket_name))
            futures.append(future)
            start_dt = next_dt
        
        completed_batches = 0
        for future in as_completed(futures):
            completed_batches += 1
            progress = (completed_batches / total_periods) * 100
            print(f"Progress: {progress:.2f}% completed")
            try:
                future.result()  # This will re-raise any exceptions caught in the thread
            except Exception as e:
                print(f"An error occurred: {e}")


def get_last_entry_date_from_v2(bucket_name):
    query_api = client_v2.query_api()
    query = f'from(bucket:"{bucket_name}") |> range(start: -1y) |> last()'
    results = query_api.query(org=INFLUXDB_V2_ORG, query=query)
    if results and len(results) > 0 and len(results[0].records) > 0:
        return results[0].records[0].get_time()
    return None


measurements = client_v1.query('SHOW MEASUREMENTS').get_points()
measurement_names = [measurement['name'] for measurement in measurements]

for measurement in measurement_names:
    
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


# for measurement in measurement_names:
#     bucket = get_or_create_bucket(measurement)
#     earliest_time, latest_time = get_time_range_for_measurement(measurement)
#     batch_write_data(measurement, earliest_time, latest_time, bucket.name, concurrency=5)
#      # Use the test function for writing top 10 data points
#     #test_write_top_10_data(measurement, bucket.name)

