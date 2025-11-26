#Sentinel Clustering Service - Flask API for Cloud Run
#Processes disease outbreak clustering with ABC and GIS algorithms


from flask import Flask, jsonify, request
from flask_cors import CORS
from google.cloud import bigquery
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
from math import radians, sin, cos, sqrt, atan2
import time
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, origins="*")

# ============================================================================
# CONFIGURATION
# ============================================================================

# BigQuery Configuration
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'sentinel-h-5')
DATASET_ID = os.environ.get('DATASET_ID', 'sentinel_h_5')

# Table names
SOURCE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.patient_records"
CLUSTERS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.clusters"
ASSIGNMENTS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.cluster_assignments"
MERGE_HISTORY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.merge_history"

# Column names (customize to match your schema)
UNIQUE_ID = os.environ.get('COL_UNIQUE_ID', 'unique_id')
PATIENT_ENTRY_DATE = os.environ.get('COL_ENTRY_DATE', 'patient_entry_date')
AREA_TYPE = os.environ.get('COL_AREA_TYPE', 'pat_areatype')
VILLAGE_NAME = os.environ.get('COL_VILLAGE', 'villagename')
CLINICAL_PRIMARY_SYNDROME = os.environ.get('COL_SYNDROME', 'clini_primary_syn')
LATITUDE = os.environ.get('COL_LATITUDE', 'latitude')
LONGITUDE = os.environ.get('COL_LONGITUDE', 'longitude')
STATE = os.environ.get('COL_STATE', 'statename')
DISTRICT = os.environ.get('COL_DISTRICT', 'districtname')
SUBDISTRICT = os.environ.get('COL_SUBDISTRICT', 'subdistrictname')

# Clustering Parameters
DBSCAN_EPSILON_M = int(os.environ.get('DBSCAN_EPSILON_M', 500))
DBSCAN_EPSILON_RADIANS = DBSCAN_EPSILON_M / 6371000
GIS_MAX_CLUSTER_RADIUS = int(os.environ.get('GIS_MAX_CLUSTER_RADIUS', 650))
AUTO_ACCEPT_RADIUS_THRESHOLD = int(os.environ.get('AUTO_ACCEPT_RADIUS', 200))
MIN_CLUSTER_SIZE = int(os.environ.get('MIN_CLUSTER_SIZE', 2))

# Window Parameters
LOOKBACK_DAYS = int(os.environ.get('LOOKBACK_DAYS', 7))
DEDUP_LOOKBACK_DAYS = int(os.environ.get('DEDUP_LOOKBACK_DAYS', 14))
MERGE_LOOKBACK_DAYS = int(os.environ.get('MERGE_LOOKBACK_DAYS', 14))
TIME_GAP_THRESHOLD = int(os.environ.get('TIME_GAP_THRESHOLD', 7))

# Threshold Parameters
GEOCODING_THRESHOLD = float(os.environ.get('GEOCODING_THRESHOLD', 0.85))
OVERLAP_MERGE_THRESHOLD = float(os.environ.get('OVERLAP_MERGE_THRESHOLD', 0.70))
OVERLAP_RELATED_THRESHOLD = float(os.environ.get('OVERLAP_RELATED_THRESHOLD', 0.30))

# Buffer Parameters
STREAMING_BUFFER_WAIT = int(os.environ.get('STREAMING_BUFFER_WAIT', 90))
BUFFER_RECHECK_INTERVAL = int(os.environ.get('BUFFER_RECHECK_INTERVAL', 10))
MAX_BUFFER_WAIT = int(os.environ.get('MAX_BUFFER_WAIT', 180))

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# ============================================================================
# TABLE CREATION
# ============================================================================

def create_tables_if_not_exist():
    """Create all required tables if they don't exist"""
    logger.info("Checking if tables exist...")
    
    # Create dataset if not exists
    dataset_ref = client.dataset(DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {DATASET_ID} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info(f"Created dataset {DATASET_ID}")
    
    # Table 1: Clusters
    clusters_schema = [
        bigquery.SchemaField("cluster_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("algorithm_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("input_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("actual_cluster_radius", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("accept_status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("merge_status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("patient_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("primary_syndrome", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("centroid_lat", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("centroid_lon", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    try:
        client.get_table(CLUSTERS_TABLE)
        logger.info(f"Table {CLUSTERS_TABLE} already exists")
    except Exception:
        table = bigquery.Table(CLUSTERS_TABLE, schema=clusters_schema)
        client.create_table(table)
        logger.info(f"Created table {CLUSTERS_TABLE}")
    
    # Table 2: Cluster Assignments
    assignments_schema = [
        bigquery.SchemaField("assignment_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cluster_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("unique_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("assigned_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    try:
        client.get_table(ASSIGNMENTS_TABLE)
        logger.info(f"Table {ASSIGNMENTS_TABLE} already exists")
    except Exception:
        table = bigquery.Table(ASSIGNMENTS_TABLE, schema=assignments_schema)
        client.create_table(table)
        logger.info(f"Created table {ASSIGNMENTS_TABLE}")
    
    # Table 3: Merge History
    merge_schema = [
        bigquery.SchemaField("merge_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("new_cluster_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("old_cluster_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("merge_decision", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("overlap_percentage", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("time_gap_days", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("distance_meters", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("performed_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    try:
        client.get_table(MERGE_HISTORY_TABLE)
        logger.info(f"Table {MERGE_HISTORY_TABLE} already exists")
    except Exception:
        table = bigquery.Table(MERGE_HISTORY_TABLE, schema=merge_schema)
        client.create_table(table)
        logger.info(f"Created table {MERGE_HISTORY_TABLE}")
    
    logger.info("All tables verified/created successfully")

# ============================================================================
# CLUSTERING FUNCTIONS
# ============================================================================

def run_complete_processing_cycle():
    """Main processing function"""
    try:
        logger.info("=" * 70)
        logger.info("Starting processing cycle")
        
        # Step 1: Pre-flight check
        if not pre_flight_check():
            return {
                'success': False,
                'message': 'Pre-flight check failed',
                'date_processed': None
            }
        
        # Step 2: Find next date
        processing_date = find_next_date()
        if not processing_date:
            return {
                'success': True,
                'message': 'All dates processed',
                'date_processed': None
            }
        
        logger.info(f"Processing date: {processing_date}")
        
        # Step 3: ABC Clustering
        abc_result = abc_clustering(processing_date)
        
        # Step 4: GIS Clustering
        gis_result = gis_clustering(processing_date)
        
        # Step 5: Wait for streaming buffer
        wait_for_buffer(processing_date, abc_result['clusters'], gis_result['clusters'])
        
        # Step 6: Merge detection
        merge_result = merge_detection(processing_date)
        
        logger.info(f"Processing complete for {processing_date}")
        
        return {
            'success': True,
            'message': 'Processing complete',
            'date_processed': processing_date,
            'abc_clusters': abc_result['clusters'],
            'gis_clusters': gis_result['clusters'],
            'gis_accepted': gis_result['accepted'],
            'gis_pending': gis_result['pending'],
            'merges': merge_result['merges'],
            'related': merge_result['related']
        }
        
    except Exception as e:
        logger.error(f"Error in processing cycle: {str(e)}", exc_info=True)
        return {
            'success': False,
            'message': f'Error: {str(e)}',
            'date_processed': None
        }

def pre_flight_check():
    """Check geocoding completeness and buffer status"""
    logger.info("Running pre-flight checks...")
    
    # Check 1: Geocoding completeness
    geocoding_query = f"""
    WITH daily_stats AS (
      SELECT 
        DATE({PATIENT_ENTRY_DATE}) as date,
        COUNT(*) as total_patients,
        COUNTIF({LATITUDE} IS NOT NULL AND {LONGITUDE} IS NOT NULL 
                AND {LATITUDE} != 0 AND {LONGITUDE} != 0 
                AND {AREA_TYPE} = 'Urban') as geocoded_urban,
        COUNTIF({AREA_TYPE} = 'Urban') as total_urban,
        CASE 
          WHEN COUNTIF({AREA_TYPE} = 'Urban') > 0 
          THEN COUNTIF({LATITUDE} IS NOT NULL AND {LONGITUDE} IS NOT NULL 
                       AND {LATITUDE} != 0 AND {LONGITUDE} != 0 
                       AND {AREA_TYPE} = 'Urban') / COUNTIF({AREA_TYPE} = 'Urban')
          ELSE 1.0 
        END as geocoding_pct
      FROM `{SOURCE_TABLE}`
      WHERE {PATIENT_ENTRY_DATE} >= DATE_SUB((SELECT MAX({PATIENT_ENTRY_DATE}) FROM `{SOURCE_TABLE}`), INTERVAL {DEDUP_LOOKBACK_DAYS} DAY)
      GROUP BY 1
    )
    SELECT 
      COUNT(*) as total_days,
      COUNTIF(geocoding_pct >= {GEOCODING_THRESHOLD}) as days_ready,
      MIN(geocoding_pct) as min_pct,
      AVG(geocoding_pct) as avg_pct
    FROM daily_stats
    """
    
    geocoding_df = client.query(geocoding_query).to_dataframe()
    
    if len(geocoding_df) == 0:
        logger.warning("No data found in source table")
        return False
    
    total_days = geocoding_df.total_days[0]
    days_ready = geocoding_df.days_ready[0]
    
    if days_ready < total_days:
        logger.warning(f"Geocoding check failed: {days_ready}/{total_days} days ready")
        return False
    
    logger.info(f"Geocoding check passed: {days_ready}/{total_days} days ready")
    
    # Check 2: Buffer status
    try:
        buffer_query = f"""
        SELECT COUNT(*) as recent_inserts
        FROM `{CLUSTERS_TABLE}`
        WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), created_at, SECOND) < 120
        """
        buffer_df = client.query(buffer_query).to_dataframe()
        recent_inserts = buffer_df.recent_inserts[0]
        
        if recent_inserts > 0:
            logger.warning(f"Buffer check: {recent_inserts} recent inserts (wait recommended)")
            return False
        
        logger.info("Buffer check passed")
    except Exception as e:
        logger.warning(f"Could not check buffer: {str(e)}")
    
    return True

def find_next_date():
    """Find the next unprocessed date"""
    query = f"""
    WITH source_date_range AS (
        SELECT 
            MIN({PATIENT_ENTRY_DATE}) AS min_date,
            MAX({PATIENT_ENTRY_DATE}) AS max_date
        FROM `{SOURCE_TABLE}`
    ),
    all_dates AS (
        SELECT date_value AS check_date
        FROM source_date_range,
        UNNEST(GENERATE_DATE_ARRAY(min_date, max_date, INTERVAL 1 DAY)) AS date_value
    ),
    processed_dates AS (
        SELECT DISTINCT input_date 
        FROM `{CLUSTERS_TABLE}`
    )
    SELECT check_date
    FROM all_dates
    LEFT JOIN processed_dates 
        ON all_dates.check_date = processed_dates.input_date
    WHERE processed_dates.input_date IS NULL
    ORDER BY check_date ASC
    LIMIT 1
    """
    
    df = client.query(query).to_dataframe()
    
    if len(df) == 0:
        logger.info("No unprocessed dates found")
        return None
    
    return df.check_date[0].strftime('%Y-%m-%d')

def abc_clustering(processing_date):
    """Run ABC clustering for rural villages"""
    logger.info("Running ABC clustering...")
    
    # Calculate windows
    frame_end = pd.to_datetime(processing_date)
    
    min_date_query = f"SELECT MIN({PATIENT_ENTRY_DATE}) AS min_date FROM `{SOURCE_TABLE}`"
    min_date_df = client.query(min_date_query).to_dataframe()
    source_min_date = min_date_df.min_date[0]
    
    calculated_start = frame_end - timedelta(days=LOOKBACK_DAYS - 1)
    frame_start = max(calculated_start.date(), source_min_date).strftime('%Y-%m-%d')
    dedup_start = (frame_end - timedelta(days=DEDUP_LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    
    # Get existing patients
    existing_query = f"""
    SELECT DISTINCT a.unique_id
    FROM `{ASSIGNMENTS_TABLE}` a
    JOIN `{CLUSTERS_TABLE}` c ON a.cluster_id = c.cluster_id
    WHERE c.algorithm_type = 'ABC'
      AND c.input_date >= DATE('{dedup_start}')
    """
    
    existing_df = client.query(existing_query).to_dataframe()
    existing_abc_patients = set(existing_df['unique_id']) if len(existing_df) > 0 else set()
    
    logger.info(f"Deduplication: {len(existing_abc_patients)} patients already in ABC clusters")
    
    # ABC query
    abc_query = f"""
    WITH base_clusters AS (
        SELECT 
            t.*,
            COUNT(*) OVER (
                PARTITION BY 
                    t.{STATE},
                    t.{DISTRICT},
                    t.{SUBDISTRICT},
                    t.{VILLAGE_NAME},
                    t.{CLINICAL_PRIMARY_SYNDROME}
            ) AS cluster_count
        FROM `{SOURCE_TABLE}` AS t
        WHERE 
            t.{PATIENT_ENTRY_DATE} BETWEEN DATE('{frame_start}') AND DATE('{processing_date}')
            AND t.{AREA_TYPE} = 'Rural'
            AND t.{VILLAGE_NAME} IS NOT NULL
    ),
    filtered_clusters AS (
        SELECT *
        FROM base_clusters
        WHERE cluster_count >= {MIN_CLUSTER_SIZE}
    ),
    clustered_data AS (
        SELECT
            t1.{UNIQUE_ID},
            t1.{CLINICAL_PRIMARY_SYNDROME},
            CONCAT(
                'ABC-',
                CAST(FARM_FINGERPRINT(CONCAT(
                    t1.{VILLAGE_NAME}, 
                    '-',
                    t1.{CLINICAL_PRIMARY_SYNDROME}
                )) AS STRING),
                '-',
                FORMAT_DATE('%d%m%y', DATE('{processing_date}'))
            ) AS cluster_id,
            'ABC' AS algorithm_type,
            DATE('{processing_date}') AS input_date,
            0.0 AS actual_cluster_radius,
            t1.cluster_count AS patient_count
        FROM filtered_clusters AS t1
    )
    SELECT 
        cluster_id,
        algorithm_type,
        input_date,
        actual_cluster_radius,
        patient_count,
        {CLINICAL_PRIMARY_SYNDROME} as primary_syndrome,
        {UNIQUE_ID},
        CONCAT(cluster_id, '-', {UNIQUE_ID}) as assignment_id
    FROM clustered_data
    """
    
    abc_df = client.query(abc_query).to_dataframe()
    
    if len(abc_df) == 0:
        logger.info("No ABC clusters found")
        return {'clusters': 0}
    
    # Filter duplicates
    abc_df = abc_df[~abc_df[UNIQUE_ID].isin(existing_abc_patients)]
    
    if len(abc_df) == 0:
        logger.info("All patients were duplicates")
        return {'clusters': 0}
    
    # Prepare inserts
    clusters_seen = set()
    clusters_to_insert = []
    assignments_to_insert = []
    
    for _, row in abc_df.iterrows():
        if row.cluster_id not in clusters_seen:
            clusters_seen.add(row.cluster_id)
            clusters_to_insert.append({
                'cluster_id': row.cluster_id,
                'algorithm_type': 'ABC',
                'input_date': row.input_date.isoformat(),
                'actual_cluster_radius': 0.0,
                'accept_status': 'Accepted',
                'merge_status': None,
                'patient_count': int(row.patient_count),
                'primary_syndrome': row.primary_syndrome,
                'centroid_lat': None,
                'centroid_lon': None,
                'created_at': datetime.now(timezone.utc).isoformat()
            })
        
        assignments_to_insert.append({
            'assignment_id': row.assignment_id,
            'cluster_id': row.cluster_id,
            'unique_id': row.unique_id,
            'assigned_at': datetime.now(timezone.utc).isoformat()
        })
    
    # Insert
    client.insert_rows_json(client.get_table(CLUSTERS_TABLE), clusters_to_insert)
    client.insert_rows_json(client.get_table(ASSIGNMENTS_TABLE), assignments_to_insert)
    
    abc_clusters_created = len(clusters_to_insert)
    logger.info(f"ABC: Created {abc_clusters_created} clusters, {len(assignments_to_insert)} assignments")
    
    return {'clusters': abc_clusters_created}

def gis_clustering(processing_date):
    """Run GIS clustering with DBSCAN + exact coordinates"""
    logger.info("Running GIS clustering...")
    
    from sklearn.cluster import DBSCAN
    from sklearn.metrics.pairwise import haversine_distances
    
    # Calculate windows
    frame_end = pd.to_datetime(processing_date)
    
    min_date_query = f"SELECT MIN({PATIENT_ENTRY_DATE}) AS min_date FROM `{SOURCE_TABLE}`"
    min_date_df = client.query(min_date_query).to_dataframe()
    source_min_date = min_date_df.min_date[0]
    
    calculated_start = frame_end - timedelta(days=LOOKBACK_DAYS - 1)
    frame_start = max(calculated_start.date(), source_min_date).strftime('%Y-%m-%d')
    dedup_start = (frame_end - timedelta(days=DEDUP_LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    
    # Get existing patients
    existing_query = f"""
    SELECT DISTINCT a.unique_id
    FROM `{ASSIGNMENTS_TABLE}` a
    JOIN `{CLUSTERS_TABLE}` c ON a.cluster_id = c.cluster_id
    WHERE c.algorithm_type = 'GIS'
      AND c.input_date >= DATE('{dedup_start}')
    """
    
    existing_df = client.query(existing_query).to_dataframe()
    existing_gis_patients = set(existing_df['unique_id']) if len(existing_df) > 0 else set()
    
    logger.info(f"Deduplication: {len(existing_gis_patients)} patients already in GIS clusters")
    
    # Extract urban patients
    extract_query = f"""
    SELECT 
        {UNIQUE_ID},
        {CLINICAL_PRIMARY_SYNDROME},
        {LATITUDE},
        {LONGITUDE}
    FROM `{SOURCE_TABLE}`
    WHERE 
        {PATIENT_ENTRY_DATE} BETWEEN DATE('{frame_start}') AND DATE('{processing_date}')
        AND {AREA_TYPE} = 'Urban'
        AND {LATITUDE} BETWEEN 8 AND 37
        AND {LONGITUDE} BETWEEN 68 AND 97
        AND {LATITUDE} != 0.0
        AND {LONGITUDE} != 0.0
        AND (pat_street IS NOT NULL AND pat_street != '' OR villagename IS NOT NULL AND villagename != '')
    """
    
    patients_df = client.query(extract_query).to_dataframe()
    
    if len(patients_df) == 0:
        logger.info("No urban patients found")
        return {'clusters': 0, 'accepted': 0, 'pending': 0}
    
    # Filter duplicates
    patients_df = patients_df[~patients_df[UNIQUE_ID].isin(existing_gis_patients)]
    
    if len(patients_df) == 0:
        logger.info("All patients were duplicates")
        return {'clusters': 0, 'accepted': 0, 'pending': 0}
    
    all_clusters = []
    all_assignments = []
    cluster_counter = 0
    total_rejected = 0
    
    syndromes = patients_df[CLINICAL_PRIMARY_SYNDROME].unique()
    dbscan_assigned_patients = set()
    
    # DBSCAN clustering
    for syndrome in syndromes:
        syndrome_df = patients_df[patients_df[CLINICAL_PRIMARY_SYNDROME] == syndrome].copy()
        
        if len(syndrome_df) < MIN_CLUSTER_SIZE:
            continue
        
        coords = syndrome_df[[LATITUDE, LONGITUDE]].values
        coords_rad = np.radians(coords)
        
        clusterer = DBSCAN(
            eps=DBSCAN_EPSILON_RADIANS,
            min_samples=MIN_CLUSTER_SIZE,
            metric='haversine'
        )
        
        cluster_labels = clusterer.fit_predict(coords_rad)
        syndrome_df['dbscan_label'] = cluster_labels
        
        for label in set(cluster_labels):
            if label == -1:
                continue
            
            cluster_patients = syndrome_df[syndrome_df['dbscan_label'] == label].copy()
            
            centroid_lat = cluster_patients[LATITUDE].mean()
            centroid_lon = cluster_patients[LONGITUDE].mean()
            
            cluster_coords = cluster_patients[[LATITUDE, LONGITUDE]].values
            centroid_coords = np.array([[centroid_lat, centroid_lon]])
            
            distances = haversine_distances(
                np.radians(cluster_coords),
                np.radians(centroid_coords)
            ) * 6371000
            
            cluster_radius = distances.max()
            
            if cluster_radius > GIS_MAX_CLUSTER_RADIUS:
                total_rejected += 1
                continue
            
            cluster_counter += 1
            cluster_id = f"GIS-{cluster_counter}-{processing_date.replace('-', '')}"
            
            accept_status = 'Accepted' if cluster_radius < AUTO_ACCEPT_RADIUS_THRESHOLD else 'Pending'
            
            all_clusters.append({
                'cluster_id': cluster_id,
                'algorithm_type': 'GIS',
                'input_date': processing_date,
                'actual_cluster_radius': float(cluster_radius),
                'accept_status': accept_status,
                'merge_status': None,
                'patient_count': int(len(cluster_patients)),
                'primary_syndrome': syndrome,
                'centroid_lat': float(centroid_lat),
                'centroid_lon': float(centroid_lon),
                'created_at': datetime.now(timezone.utc).isoformat()
            })
            
            for idx, patient in cluster_patients.iterrows():
                all_assignments.append({
                    'assignment_id': f"{cluster_id}-{patient[UNIQUE_ID]}",
                    'cluster_id': cluster_id,
                    'unique_id': patient[UNIQUE_ID],
                    'assigned_at': datetime.now(timezone.utc).isoformat()
                })
                dbscan_assigned_patients.add(patient[UNIQUE_ID])
    
    dbscan_clusters = cluster_counter
    
    # Exact coordinate clustering for unassigned
    unassigned_df = patients_df[~patients_df[UNIQUE_ID].isin(dbscan_assigned_patients)].copy()
    
    if len(unassigned_df) > 0:
        syndromes_unassigned = unassigned_df[CLINICAL_PRIMARY_SYNDROME].unique()
        
        for syndrome in syndromes_unassigned:
            syndrome_df = unassigned_df[unassigned_df[CLINICAL_PRIMARY_SYNDROME] == syndrome].copy()
            coord_groups = syndrome_df.groupby([LATITUDE, LONGITUDE])
            
            for (lat, lon), group in coord_groups:
                if len(group) >= MIN_CLUSTER_SIZE:
                    cluster_counter += 1
                    cluster_id = f"GIS-{cluster_counter}-{processing_date.replace('-', '')}"
                    
                    all_clusters.append({
                        'cluster_id': cluster_id,
                        'algorithm_type': 'GIS',
                        'input_date': processing_date,
                        'actual_cluster_radius': 0.0,
                        'accept_status': 'Accepted',
                        'merge_status': None,
                        'patient_count': int(len(group)),
                        'primary_syndrome': syndrome,
                        'centroid_lat': float(lat),
                        'centroid_lon': float(lon),
                        'created_at': datetime.now(timezone.utc).isoformat()
                    })
                    
                    for idx, patient in group.iterrows():
                        all_assignments.append({
                            'assignment_id': f"{cluster_id}-{patient[UNIQUE_ID]}",
                            'cluster_id': cluster_id,
                            'unique_id': patient[UNIQUE_ID],
                            'assigned_at': datetime.now(timezone.utc).isoformat()
                        })
    
    # Insert to BigQuery
    if len(all_clusters) > 0:
        client.insert_rows_json(client.get_table(CLUSTERS_TABLE), all_clusters)
        client.insert_rows_json(client.get_table(ASSIGNMENTS_TABLE), all_assignments)
        
        gis_clusters_created = len(all_clusters)
        gis_accepted = sum(1 for c in all_clusters if c['accept_status'] == 'Accepted')
        gis_pending = gis_clusters_created - gis_accepted
        
        logger.info(f"GIS: Created {gis_clusters_created} clusters ({gis_accepted} accepted, {gis_pending} pending)")
        
        return {'clusters': gis_clusters_created, 'accepted': gis_accepted, 'pending': gis_pending}
    
    return {'clusters': 0, 'accepted': 0, 'pending': 0}

def wait_for_buffer(processing_date, abc_expected, gis_expected):
    """Wait for streaming buffer to clear"""
    logger.info(f"Waiting {STREAMING_BUFFER_WAIT}s for streaming buffer...")
    time.sleep(STREAMING_BUFFER_WAIT)
    
    # Verify data visibility
    query = f"""
    SELECT 
        COUNTIF(algorithm_type = 'ABC' AND input_date = DATE('{processing_date}')) as abc_clusters,
        COUNTIF(algorithm_type = 'GIS' AND input_date = DATE('{processing_date}')) as gis_clusters
    FROM `{CLUSTERS_TABLE}`
    """
    
    df = client.query(query).to_dataframe()
    abc_visible = df.abc_clusters[0]
    gis_visible = df.gis_clusters[0]
    
    total_wait = STREAMING_BUFFER_WAIT
    
    while (abc_visible < abc_expected or gis_visible < gis_expected) and total_wait < MAX_BUFFER_WAIT:
        logger.info(f"Data not fully visible - waiting {BUFFER_RECHECK_INTERVAL}s more...")
        time.sleep(BUFFER_RECHECK_INTERVAL)
        total_wait += BUFFER_RECHECK_INTERVAL
        
        df = client.query(query).to_dataframe()
        abc_visible = df.abc_clusters[0]
        gis_visible = df.gis_clusters[0]
    
    logger.info(f"Buffer wait complete: {total_wait}s (ABC: {abc_visible}/{abc_expected}, GIS: {gis_visible}/{gis_expected})")

def merge_detection(processing_date):
    """Detect and log cluster merges"""
    logger.info("Running merge detection...")
    
    lookback_start = (pd.to_datetime(processing_date) - timedelta(days=MERGE_LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    
    # Get today's clusters
    today_query = f"""
    SELECT 
        c.cluster_id,
        c.algorithm_type,
        c.primary_syndrome,
        c.centroid_lat,
        c.centroid_lon,
        ARRAY_AGG(a.unique_id) as patient_list
    FROM `{CLUSTERS_TABLE}` c
    JOIN `{ASSIGNMENTS_TABLE}` a ON c.cluster_id = a.cluster_id
    WHERE c.input_date = DATE('{processing_date}')
      AND c.algorithm_type IN ('ABC', 'GIS')
    GROUP BY 1, 2, 3, 4, 5
    """
    
    today_df = client.query(today_query).to_dataframe()
    
    if len(today_df) == 0:
        logger.info("No clusters to merge")
        return {'merges': 0, 'related': 0}
    
    # Get previous clusters
    previous_query = f"""
    SELECT 
        c.cluster_id,
        c.algorithm_type,
        c.input_date,
        c.primary_syndrome,
        c.centroid_lat,
        c.centroid_lon,
        ARRAY_AGG(a.unique_id) as patient_list
    FROM `{CLUSTERS_TABLE}` c
    JOIN `{ASSIGNMENTS_TABLE}` a ON c.cluster_id = a.cluster_id
    WHERE c.input_date BETWEEN DATE('{lookback_start}') AND DATE_SUB(DATE('{processing_date}'), INTERVAL 1 DAY)
      AND c.algorithm_type IN ('ABC', 'GIS')
      AND (c.merge_status IS NULL OR c.merge_status = 'Parent')
    GROUP BY 1, 2, 3, 4, 5, 6
    """
    
    previous_df = client.query(previous_query).to_dataframe()
    
    if len(previous_df) == 0:
        logger.info("No previous clusters to merge with")
        return {'merges': 0, 'related': 0}
    
    merge_actions = []
    related_actions = []
    
    # Check overlaps
    for _, new_cluster in today_df.iterrows():
        new_patients = set(new_cluster['patient_list'])
        new_syndrome = new_cluster['primary_syndrome']
        new_algorithm = new_cluster['algorithm_type']
        new_id = new_cluster['cluster_id']
        
        candidates = previous_df[
            (previous_df['primary_syndrome'] == new_syndrome) &
            (previous_df['algorithm_type'] == new_algorithm)
        ]
        
        for _, old_cluster in candidates.iterrows():
            old_patients = set(old_cluster['patient_list'])
            old_id = old_cluster['cluster_id']
            old_date = old_cluster['input_date']
            
            time_gap = (pd.to_datetime(processing_date) - pd.to_datetime(old_date)).days
            
            if time_gap > TIME_GAP_THRESHOLD:
                continue
            
            overlap_patients = new_patients & old_patients
            overlap_count = len(overlap_patients)
            
            if overlap_count == 0:
                continue
            
            smaller_size = min(len(new_patients), len(old_patients))
            overlap_pct = overlap_count / smaller_size
            
            # Calculate distance for GIS
            if new_algorithm == 'GIS':
                lat1, lon1 = radians(new_cluster['centroid_lat']), radians(new_cluster['centroid_lon'])
                lat2, lon2 = radians(old_cluster['centroid_lat']), radians(old_cluster['centroid_lon'])
                
                dlat = lat2 - lat1
                dlon = lon2 - lon1
                a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
                c = 2 * atan2(sqrt(a), sqrt(1-a))
                distance_m = 6371000 * c
            else:
                distance_m = 0.0
            
            # Decision
            if overlap_pct >= OVERLAP_MERGE_THRESHOLD:
                merge_actions.append({
                    'new_cluster_id': new_id,
                    'old_cluster_id': old_id,
                    'algorithm_type': new_algorithm,
                    'overlap_count': overlap_count,
                    'overlap_pct': overlap_pct,
                    'time_gap_days': time_gap,
                    'distance_m': distance_m
                })
            elif overlap_pct >= OVERLAP_RELATED_THRESHOLD:
                related_actions.append({
                    'new_cluster_id': new_id,
                    'old_cluster_id': old_id,
                    'algorithm_type': new_algorithm,
                    'overlap_count': overlap_count,
                    'overlap_pct': overlap_pct,
                    'time_gap_days': time_gap,
                    'distance_m': distance_m
                })
    
    # Log merges
    if len(merge_actions) > 0:
        for action in merge_actions:
            merge_record = {
                'merge_id': f"M-{action['new_cluster_id']}-{action['old_cluster_id']}",
                'new_cluster_id': action['new_cluster_id'],
                'old_cluster_id': action['old_cluster_id'],
                'merge_decision': 'AUTO_MERGE',
                'overlap_percentage': float(action['overlap_pct']),
                'time_gap_days': int(action['time_gap_days']),
                'distance_meters': float(action['distance_m']),
                'performed_at': datetime.now(timezone.utc).isoformat()
            }
            client.insert_rows_json(client.get_table(MERGE_HISTORY_TABLE), [merge_record])
    
    # Log related
    if len(related_actions) > 0:
        for action in related_actions:
            related_record = {
                'merge_id': f"R-{action['new_cluster_id']}-{action['old_cluster_id']}",
                'new_cluster_id': action['new_cluster_id'],
                'old_cluster_id': action['old_cluster_id'],
                'merge_decision': 'RELATED',
                'overlap_percentage': float(action['overlap_pct']),
                'time_gap_days': int(action['time_gap_days']),
                'distance_meters': float(action['distance_m']),
                'performed_at': datetime.now(timezone.utc).isoformat()
            }
            client.insert_rows_json(client.get_table(MERGE_HISTORY_TABLE), [related_record])
    
    logger.info(f"Merge detection: {len(merge_actions)} merges, {len(related_actions)} related")
    
    return {'merges': len(merge_actions), 'related': len(related_actions)}

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/', methods=['GET'])
def index():
    """Welcome endpoint"""
    return jsonify({
        'service': 'Sentinel Clustering API',
        'version': '1.0.0',
        'status': 'running',
        'endpoints': {
            'health': '/health',
            'process': '/process (POST)',
            'status': '/status',
            'config': '/config'
        }
    })

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    try:
        # Test BigQuery connection
        client.query("SELECT 1").result()
        return jsonify({
            'status': 'healthy',
            'bigquery': 'connected',
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 500

@app.route('/init', methods=['POST'])
def init_tables():
    """Initialize tables (create if not exist)"""
    try:
        create_tables_if_not_exist()
        return jsonify({
            'success': True,
            'message': 'Tables initialized successfully',
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
    except Exception as e:
        logger.error(f"Error initializing tables: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 500

@app.route('/process', methods=['POST'])
def process():
    """Process next date"""
    try:
        result = run_complete_processing_cycle()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in /process: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 500

@app.route('/status', methods=['GET'])
def status():
    """Get processing status"""
    try:
        # Count processed dates
        query = f"""
        SELECT 
            COUNT(DISTINCT input_date) as dates_processed,
            MAX(input_date) as last_processed_date,
            SUM(CASE WHEN algorithm_type = 'ABC' THEN 1 ELSE 0 END) as total_abc_clusters,
            SUM(CASE WHEN algorithm_type = 'GIS' THEN 1 ELSE 0 END) as total_gis_clusters
        FROM `{CLUSTERS_TABLE}`
        """
        df = client.query(query).to_dataframe()
        
        return jsonify({
            'dates_processed': int(df.dates_processed[0]) if len(df) > 0 and pd.notna(df.dates_processed[0]) else 0,
            'last_processed_date': df.last_processed_date[0].isoformat() if len(df) > 0 and pd.notna(df.last_processed_date[0]) else None,
            'total_abc_clusters': int(df.total_abc_clusters[0]) if len(df) > 0 and pd.notna(df.total_abc_clusters[0]) else 0,
            'total_gis_clusters': int(df.total_gis_clusters[0]) if len(df) > 0 and pd.notna(df.total_gis_clusters[0]) else 0,
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
    except Exception as e:
        logger.error(f"Error in /status: {str(e)}", exc_info=True)
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 500

@app.route('/preflight', methods=['GET'])
def preflight_check_endpoint():
    """Standalone preflight check endpoint"""
    try:
        logger.info("Running standalone preflight check...")
        
        # Check 1: Geocoding completeness
        geocoding_query = f"""
        WITH daily_stats AS (
          SELECT 
            DATE({PATIENT_ENTRY_DATE}) as date,
            COUNT(*) as total_patients,
            COUNTIF({LATITUDE} IS NOT NULL AND {LONGITUDE} IS NOT NULL 
                    AND {LATITUDE} != 0 AND {LONGITUDE} != 0 
                    AND {AREA_TYPE} = 'Urban') as geocoded_urban,
            COUNTIF({AREA_TYPE} = 'Urban') as total_urban,
            CASE 
              WHEN COUNTIF({AREA_TYPE} = 'Urban') > 0 
              THEN COUNTIF({LATITUDE} IS NOT NULL AND {LONGITUDE} IS NOT NULL 
                           AND {LATITUDE} != 0 AND {LONGITUDE} != 0 
                           AND {AREA_TYPE} = 'Urban') / COUNTIF({AREA_TYPE} = 'Urban')
              ELSE 1.0 
            END as geocoding_pct
          FROM `{SOURCE_TABLE}`
          WHERE {PATIENT_ENTRY_DATE} >= DATE_SUB((SELECT MAX({PATIENT_ENTRY_DATE}) FROM `{SOURCE_TABLE}`), INTERVAL {DEDUP_LOOKBACK_DAYS} DAY)
          GROUP BY 1
          ORDER BY 1 DESC
        )
        SELECT 
          COUNT(*) as total_days,
          COUNTIF(geocoding_pct >= {GEOCODING_THRESHOLD}) as days_ready,
          MIN(geocoding_pct) as min_pct,
          AVG(geocoding_pct) as avg_pct,
          ARRAY_AGG(STRUCT(date, geocoding_pct, total_urban, geocoded_urban) ORDER BY date DESC LIMIT 10) as recent_days
        FROM daily_stats
        """
        
        geocoding_df = client.query(geocoding_query).to_dataframe()
        
        if len(geocoding_df) == 0:
            return jsonify({
                'success': False,
                'geocoding_check': False,
                'buffer_check': False,
                'message': 'No data found in source table',
                'details': {}
            })
        
        row = geocoding_df.iloc[0]
        total_days = row.total_days
        days_ready = row.days_ready
        min_pct = row.min_pct
        avg_pct = row.avg_pct
        
        geocoding_passed = bool(days_ready == total_days)
        
        # Check 2: Buffer status
        buffer_passed = True
        buffer_info = {}
        try:
            buffer_query = f"""
            SELECT COUNT(*) as recent_inserts
            FROM `{CLUSTERS_TABLE}`
            WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), created_at, SECOND) < 120
            """
            buffer_df = client.query(buffer_query).to_dataframe()
            recent_inserts = buffer_df.recent_inserts[0]
            
            if recent_inserts > 0:
                buffer_passed = False
                buffer_info = {'recent_inserts': int(recent_inserts)}
        except Exception as e:
            buffer_info = {'error': str(e)}
        
        overall_passed = bool(geocoding_passed and buffer_passed)
        
        return jsonify({
            'success': True,
            'overall_passed': overall_passed,
            'geocoding_check': geocoding_passed,
            'buffer_check': buffer_passed,
            'details': {
                'total_days_checked': int(total_days),
                'days_ready': int(days_ready),
                'min_daily_pct': float(min_pct) * 100,
                'avg_daily_pct': float(avg_pct) * 100,
                'threshold_required': GEOCODING_THRESHOLD * 100,
                'buffer_info': buffer_info
            },
            'message': 'Preflight check completed' if overall_passed else 'Preflight check failed',
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in preflight check: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 500

@app.route('/config', methods=['GET'])
def config():
    """Get current configuration"""
    return jsonify({
        'project_id': PROJECT_ID,
        'dataset_id': DATASET_ID,
        'parameters': {
            'dbscan_epsilon_m': DBSCAN_EPSILON_M,
            'gis_max_cluster_radius': GIS_MAX_CLUSTER_RADIUS,
            'auto_accept_radius': AUTO_ACCEPT_RADIUS_THRESHOLD,
            'min_cluster_size': MIN_CLUSTER_SIZE,
            'lookback_days': LOOKBACK_DAYS,
            'dedup_lookback_days': DEDUP_LOOKBACK_DAYS,
            'merge_lookback_days': MERGE_LOOKBACK_DAYS,
            'geocoding_threshold': GEOCODING_THRESHOLD,
            'overlap_merge_threshold': OVERLAP_MERGE_THRESHOLD,
            'overlap_related_threshold': OVERLAP_RELATED_THRESHOLD
        },
        'timestamp': datetime.now(timezone.utc).isoformat()
    })

@app.route('/test', methods=['POST'])
def test_clustering():
    """Comprehensive test function - validates all steps without writing to BigQuery"""
    try:
        logger.info("Starting comprehensive clustering test...")
        
        # Get last 3 processed dates for comparison
        last_dates_query = f"""
        SELECT DISTINCT input_date 
        FROM `{CLUSTERS_TABLE}`
        ORDER BY input_date DESC
        LIMIT 3
        """
        last_dates_df = client.query(last_dates_query).to_dataframe()
        last_dates = [d.strftime('%Y-%m-%d') for d in last_dates_df.input_date] if len(last_dates_df) > 0 else []
        
        # Find next date to test
        next_date = find_next_date()
        if not next_date:
            return jsonify({
                'success': False,
                'message': 'No unprocessed dates available for testing'
            })
        
        test_results = {
            'test_date': next_date,
            'comparison_dates': last_dates,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'steps': {},
            'inconsistencies': [],
            'summary': {}
        }
        
        # Step 1: Pre-flight validation with geocoding inconsistency check
        geocoding_query = f"""
        WITH daily_stats AS (
          SELECT 
            DATE({PATIENT_ENTRY_DATE}) as date,
            COUNTIF({AREA_TYPE} = 'Urban') as total_urban,
            COUNTIF({LATITUDE} IS NOT NULL AND {LONGITUDE} IS NOT NULL 
                    AND {LATITUDE} != 0 AND {LONGITUDE} != 0 
                    AND {AREA_TYPE} = 'Urban') as geocoded_urban_reported,
            COUNTIF({LATITUDE} IS NOT NULL AND {LONGITUDE} IS NOT NULL 
                    AND {LATITUDE} != 0 AND {LONGITUDE} != 0 
                    AND {AREA_TYPE} = 'Urban'
                    AND (pat_street IS NOT NULL AND pat_street != '' OR villagename IS NOT NULL AND villagename != '')) as geocoded_urban_usable
          FROM `{SOURCE_TABLE}`
          WHERE {PATIENT_ENTRY_DATE} >= DATE_SUB(DATE('{next_date}'), INTERVAL {DEDUP_LOOKBACK_DAYS} DAY)
            AND {PATIENT_ENTRY_DATE} <= DATE('{next_date}')
          GROUP BY 1
        )
        SELECT 
          SUM(total_urban) as total_urban_records,
          SUM(geocoded_urban_reported) as total_geocoded_reported,
          SUM(geocoded_urban_usable) as total_geocoded_usable,
          ROUND(SUM(geocoded_urban_reported) * 100.0 / SUM(total_urban), 2) as reported_percentage,
          ROUND(SUM(geocoded_urban_usable) * 100.0 / SUM(total_urban), 2) as usable_percentage
        FROM daily_stats
        WHERE total_urban > 0
        """
        
        geocoding_df = client.query(geocoding_query).to_dataframe()
        
        if len(geocoding_df) > 0:
            row = geocoding_df.iloc[0]
            inconsistency_detected = abs(row.reported_percentage - row.usable_percentage) > 5.0
            
            test_results['steps']['geocoding_validation'] = {
                'total_urban_records': int(row.total_urban_records),
                'geocoded_reported': int(row.total_geocoded_reported),
                'geocoded_usable': int(row.total_geocoded_usable),
                'reported_percentage': float(row.reported_percentage),
                'usable_percentage': float(row.usable_percentage),
                'inconsistency_detected': bool(inconsistency_detected)
            }
            
            if inconsistency_detected:
                test_results['inconsistencies'].append({
                    'type': 'geocoding_threshold_inconsistency',
                    'message': f'Reported: {row.reported_percentage}% vs Usable: {row.usable_percentage}%',
                    'severity': 'critical'
                })
        
        # Step 2: Address filtering validation
        filter_query = f"""
        SELECT 
            COUNT(*) as total_urban,
            COUNTIF((pat_street IS NOT NULL AND pat_street != '') OR (villagename IS NOT NULL AND villagename != '')) as has_address,
            COUNTIF({LATITUDE} IS NOT NULL AND {LONGITUDE} IS NOT NULL AND {LATITUDE} != 0 AND {LONGITUDE} != 0) as has_coordinates
        FROM `{SOURCE_TABLE}`
        WHERE {PATIENT_ENTRY_DATE} = DATE('{next_date}')
          AND {AREA_TYPE} = 'Urban'
        """
        
        filter_df = client.query(filter_query).to_dataframe()
        
        if len(filter_df) > 0:
            row = filter_df.iloc[0]
            test_results['steps']['address_filtering'] = {
                'total_urban': int(row.total_urban),
                'has_address': int(row.has_address),
                'has_coordinates': int(row.has_coordinates),
                'filter_efficiency': float(round(row.has_address * 100.0 / row.total_urban, 2)) if row.total_urban > 0 else 0.0
            }
        
        # Step 3: Historical comparison
        if last_dates:
            history_query = f"""
            SELECT 
                input_date,
                algorithm_type,
                COUNT(*) as cluster_count
            FROM `{CLUSTERS_TABLE}`
            WHERE input_date IN UNNEST(@dates)
            GROUP BY 1, 2
            ORDER BY 1 DESC, 2
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter('dates', 'DATE', [pd.to_datetime(d).date() for d in last_dates])
                ]
            )
            
            history_df = client.query(history_query, job_config=job_config).to_dataframe()
            
            if len(history_df) > 0:
                abc_counts = history_df[history_df['algorithm_type'] == 'ABC']['cluster_count'].tolist()
                gis_counts = history_df[history_df['algorithm_type'] == 'GIS']['cluster_count'].tolist()
                
                test_results['steps']['historical_comparison'] = {
                    'abc_cluster_counts': abc_counts,
                    'gis_cluster_counts': gis_counts,
                    'abc_variance': float(np.var(abc_counts)) if len(abc_counts) > 1 else 0,
                    'gis_variance': float(np.var(gis_counts)) if len(gis_counts) > 1 else 0
                }
        
        # Generate summary
        critical_issues = len([i for i in test_results['inconsistencies'] if i.get('severity') == 'critical'])
        test_results['summary'] = {
            'overall_status': 'failed' if critical_issues > 0 else 'passed',
            'total_inconsistencies': len(test_results['inconsistencies']),
            'critical_issues': critical_issues
        }
        
        logger.info("Comprehensive test completed")
        return jsonify({
            'success': True,
            'data': test_results
        })
        
    except Exception as e:
        logger.error(f"Error in test function: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)