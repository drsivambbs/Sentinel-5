import pandas as pd
from google.cloud import bigquery, secretmanager
import functions_framework
import requests
import re
import time
import os

# Environment variables
PROJECT_ID = os.getenv('PROJECT_ID', 'sentinel-h-5')
DATASET_ID = os.getenv('DATASET_ID', 'sentinel_h_5')
TABLE_ID = os.getenv('TABLE_ID', 'patient_records')
CACHE_TABLE = os.getenv('CACHE_TABLE', 'sentinel_h_5.geocode_cache')
SECRET_NAME = os.getenv('GEOCODING_SECRET_NAME', 'google_map_api_key')


@functions_framework.http
def geocode_addresses(request):
    """Cloud Function: Geocode null latitude/longitude records in BigQuery"""
    
    # Initialize clients
    bq_client = bigquery.Client(project=PROJECT_ID)
    secret_client = secretmanager.SecretManagerServiceClient()
    
    # Get API key from Secret Manager
    try:
        secret_path = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_path})
        api_key = response.payload.data.decode("UTF-8")
    except Exception as e:
        return {"error": f"Failed to get API key: {str(e)}"}, 500
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Fetch records with null latitude/longitude (limit 100 per batch)
    query = f"""
        SELECT unique_id, pat_street, pat_house, villagename, subdistrictname, districtname, statename, pat_pincode
        FROM `{table_id}`
        WHERE (latitude IS NULL OR longitude IS NULL)
        AND (pat_street IS NOT NULL OR pat_house IS NOT NULL OR villagename IS NOT NULL)
        LIMIT 100
    """
    
    try:
        df = bq_client.query(query).to_dataframe()
        if df.empty:
            return {"message": "No records need geocoding"}, 200
        
        # Check if table can be updated (no streaming buffer)
        try:
            test_update = f"UPDATE `{table_id}` SET latitude = latitude WHERE unique_id = 'non_existent_test_id_12345'"
            bq_client.query(test_update).result()
        except Exception as e:
            if "streaming buffer" in str(e).lower():
                return {"message": "Table has streaming buffer, skipping to avoid API waste"}, 200
            # If it's not a streaming buffer error, continue
        
        # Geocode batch with persistent cache
        geocoded_batch = geocode_batch(df, api_key, bq_client)
        total_geocoded = len(geocoded_batch)
        
        # Update BigQuery table
        if total_geocoded > 0:
            update_records(bq_client, table_id, geocoded_batch)
        
        return {"message": f"Geocoded {total_geocoded} records"}, 200
    
    except Exception as e:
        if "streaming buffer" in str(e).lower():
            return {"message": "Table has streaming buffer, skipping to avoid API waste"}, 200
        return {"error": f"Geocoding failed: {str(e)}"}, 500


def clean_address_line(pat_street, pat_house, villagename, subdistrictname, districtname, statename, pat_pincode):
    """Clean messy Indian addresses for geocoding"""
    parts = []
    for part in [pat_house, pat_street, villagename, subdistrictname, districtname, statename, pat_pincode]:
        if part is not None and pd.notna(part) and str(part).strip():
            part = str(part).strip().strip('"')
            # Keep letters, digits, spaces, comma, dot, hyphen
            part = re.sub(r'[^\w\s,.-]', '', part, flags=re.UNICODE)
            if part:
                parts.append(part)
    parts.append("India")  # bias geocoding to India
    return ", ".join(parts)


def is_valid_indian_location(lat, lng):
    """Check if coordinates are within India bounding box"""
    return (6.0 <= lat <= 37.0) and (68.0 <= lng <= 97.0)


def get_cached_coordinates(bq_client, addresses):
    """Check BigQuery persistent cache for existing addresses"""
    if not addresses:
        return {}
    query = f"""
        SELECT full_address, latitude, longitude
        FROM `{PROJECT_ID}.{CACHE_TABLE}`
        WHERE full_address IN UNNEST(@addresses)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("addresses", "STRING", addresses)]
    )
    df = bq_client.query(query, job_config=job_config).to_dataframe()
    return {row['full_address']: (row['latitude'], row['longitude']) for _, row in df.iterrows()}


def update_cache(bq_client, new_records):
    """Insert newly geocoded addresses into cache"""
    if not new_records:
        return
    rows_to_insert = [
        {"full_address": r['full_address'], "latitude": r['latitude'], "longitude": r['longitude']}
        for r in new_records
    ]
    bq_client.insert_rows_json(f"{PROJECT_ID}.{CACHE_TABLE}", rows_to_insert)


def geocode_batch(batch, api_key, bq_client):
    """Geocode a batch of addresses with persistent cache"""
    geocoded_records = []
    new_cache_records = []

    # Prepare cleaned addresses
    batch['full_address'] = batch.apply(lambda r: clean_address_line(
        r['pat_street'], r['pat_house'], r['villagename'], r['subdistrictname'], 
        r['districtname'], r['statename'], r['pat_pincode']), axis=1)

    addresses = batch['full_address'].tolist()
    cache = get_cached_coordinates(bq_client, addresses)

    for _, row in batch.iterrows():
        addr = row['full_address']

        # Use cached coordinates if available
        if addr in cache:
            lat, lng = cache[addr]
            geocoded_records.append({'unique_id': row['unique_id'], 'latitude': lat, 'longitude': lng})
            continue

        # Call Google Maps API
        try:
            url = "https://maps.googleapis.com/maps/api/geocode/json"
            params = {'address': addr, 'key': api_key, 'components': 'country:IN'}
            response = requests.get(url, params=params, timeout=10)
            data = response.json()

            if data['status'] == 'OK' and data['results']:
                for result in data['results']:
                    loc = result['geometry']['location']
                    if is_valid_indian_location(loc['lat'], loc['lng']):
                        geocoded_records.append({
                            'unique_id': row['unique_id'],
                            'latitude': loc['lat'],
                            'longitude': loc['lng']
                        })
                        new_cache_records.append({'full_address': addr, 'latitude': loc['lat'], 'longitude': loc['lng']})
                        break
            time.sleep(0.05)  # rate limiting

        except Exception as e:
            print(f"Error geocoding {row['unique_id']}: {str(e)}")
            continue

    # Update persistent cache
    update_cache(bq_client, new_cache_records)
    return geocoded_records


def update_records(bq_client, table_id, geocoded_records):
    """Update BigQuery table with geocoded latitude/longitude"""
    if not geocoded_records:
        return

    lat_cases = " ".join([f"WHEN '{r['unique_id']}' THEN {r['latitude']}" for r in geocoded_records])
    lng_cases = " ".join([f"WHEN '{r['unique_id']}' THEN {r['longitude']}" for r in geocoded_records])
    unique_ids = [r['unique_id'] for r in geocoded_records]

    update_query = f"""
        UPDATE `{table_id}`
        SET
            latitude = CASE unique_id {lat_cases} ELSE latitude END,
            longitude = CASE unique_id {lng_cases} ELSE longitude END
        WHERE unique_id IN UNNEST(@unique_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("unique_ids", "STRING", unique_ids)]
    )
    bq_client.query(update_query, job_config=job_config).result()