# Smart Clustering Engine - Enhanced outbreak continuity tracking
# Implements time-based and geographic merging without overlap thresholds

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
import uuid

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
SMART_CLUSTERS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.smart_clusters"
SMART_ASSIGNMENTS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.smart_cluster_assignments"
SMART_MERGE_HISTORY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.smart_merge_history"

# Column names
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

# Smart Clustering Parameters
DBSCAN_EPSILON_M = int(os.environ.get('DBSCAN_EPSILON_M', 500))
DBSCAN_EPSILON_RADIANS = DBSCAN_EPSILON_M / 6371000
MAX_CLUSTER_RADIUS = int(os.environ.get('MAX_CLUSTER_RADIUS', 800))
MIN_CLUSTER_SIZE = int(os.environ.get('MIN_CLUSTER_SIZE', 2))
TIME_WINDOW_DAYS = int(os.environ.get('TIME_WINDOW_DAYS', 6))
MAX_CLUSTER_AGE_DAYS = int(os.environ.get('MAX_CLUSTER_AGE_DAYS', 7))
LOOKBACK_DAYS = int(os.environ.get('LOOKBACK_DAYS', 7))
GEOCODING_THRESHOLD = float(os.environ.get('GEOCODING_THRESHOLD', 0.85))
STREAMING_BUFFER_WAIT = int(os.environ.get('STREAMING_BUFFER_WAIT', 90))
AUTO_ACCEPT_RADIUS_THRESHOLD = int(os.environ.get('AUTO_ACCEPT_RADIUS', 200))

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# ============================================================================
# TABLE CREATION
# ============================================================================

def create_smart_tables():
    """Create smart clustering tables"""
    logger.info("Creating smart clustering tables...")
    
    # Create dataset if not exists
    dataset_ref = client.dataset(DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info(f"Created dataset {DATASET_ID}")
    
    # Smart Clusters Table
    smart_clusters_schema = [
        bigquery.SchemaField("smart_cluster_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("algorithm_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("input_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("original_creation_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("actual_cluster_radius", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("accept_status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("patient_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("primary_syndrome", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("centroid_lat", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("centroid_lon", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("village_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("expansion_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    try:
        client.get_table(SMART_CLUSTERS_TABLE)
        logger.info("Smart clusters table exists")
    except Exception:
        table = bigquery.Table(SMART_CLUSTERS_TABLE, schema=smart_clusters_schema)
        client.create_table(table)
        logger.info("Created smart_clusters table")
    
    # Smart Assignments Table
    smart_assignments_schema = [
        bigquery.SchemaField("assignment_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("smart_cluster_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("unique_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("assigned_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("addition_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("expansion_date", "DATE", mode="NULLABLE"),
    ]
    
    try:
        client.get_table(SMART_ASSIGNMENTS_TABLE)
        logger.info("Smart assignments table exists")
    except Exception:
        table = bigquery.Table(SMART_ASSIGNMENTS_TABLE, schema=smart_assignments_schema)
        client.create_table(table)
        logger.info("Created smart_cluster_assignments table")
    
    # Smart Merge History Table
    smart_merge_schema = [
        bigquery.SchemaField("merge_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("target_cluster_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_cluster_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("merge_reason", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cases_added", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("overlap_cases_removed", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("performed_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    try:
        client.get_table(SMART_MERGE_HISTORY_TABLE)
        logger.info("Smart merge history table exists")
    except Exception:
        table = bigquery.Table(SMART_MERGE_HISTORY_TABLE, schema=smart_merge_schema)
        client.create_table(table)
        logger.info("Created smart_merge_history table")
    
    # Smart Processing Status Table
    SMART_PROCESSING_STATUS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.smart_processing_status"
    processing_status_schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),  # IN_PROGRESS, COMPLETED, FAILED
        bigquery.SchemaField("worker_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("completed_at", "TIMESTAMP", mode="NULLABLE"),
    ]
    
    try:
        client.get_table(SMART_PROCESSING_STATUS_TABLE)
        logger.info("Smart processing status table exists")
    except Exception:
        table = bigquery.Table(SMART_PROCESSING_STATUS_TABLE, schema=processing_status_schema)
        client.create_table(table)
        logger.info("Created smart_processing_status table")

# ============================================================================
# SMART CLUSTERING FUNCTIONS
# ============================================================================

def claim_next_date():
    """Atomically claim next unprocessed date"""
    worker_id = f"worker-{uuid.uuid4().hex[:8]}"
    
    # Find next unprocessed date
    query = f"""
    WITH source_dates AS (
        SELECT DISTINCT {PATIENT_ENTRY_DATE} as date
        FROM `{SOURCE_TABLE}`
    ),
    processed_dates AS (
        SELECT DISTINCT date
        FROM `{PROJECT_ID}.{DATASET_ID}.smart_processing_status`
        WHERE status IN ('IN_PROGRESS', 'COMPLETED')
    )
    SELECT s.date
    FROM source_dates s
    LEFT JOIN processed_dates p ON s.date = p.date
    WHERE p.date IS NULL
    ORDER BY s.date ASC
    LIMIT 1
    """
    
    df = client.query(query).to_dataframe()
    if len(df) == 0:
        return None, None
    
    next_date = df.date[0].strftime('%Y-%m-%d')
    
    # Try to claim the date atomically
    try:
        claim_query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.smart_processing_status`
        (date, status, worker_id, started_at)
        VALUES (@date, 'IN_PROGRESS', @worker_id, @timestamp)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('date', 'DATE', next_date),
                bigquery.ScalarQueryParameter('worker_id', 'STRING', worker_id),
                bigquery.ScalarQueryParameter('timestamp', 'TIMESTAMP', datetime.now(timezone.utc))
            ]
        )
        
        client.query(claim_query, job_config=job_config)
        logger.info(f"Worker {worker_id} claimed date {next_date}")
        return next_date, worker_id
        
    except Exception as e:
        logger.info(f"Failed to claim date {next_date}: {str(e)}")
        return None, None

def check_data_quality(processing_date):
    """Check geocoding quality and streaming buffer before processing"""
    logger.info(f"Checking data quality for {processing_date}")
    
    # Check geocoding completeness
    geocoding_query = f"""
    SELECT 
        COUNT(*) as total_urban,
        COUNTIF({LATITUDE} IS NOT NULL AND {LONGITUDE} IS NOT NULL 
                AND {LATITUDE} != 0 AND {LONGITUDE} != 0) as geocoded_urban,
        CASE 
            WHEN COUNT(*) > 0 
            THEN COUNTIF({LATITUDE} IS NOT NULL AND {LONGITUDE} IS NOT NULL 
                         AND {LATITUDE} != 0 AND {LONGITUDE} != 0) / COUNT(*)
            ELSE 1.0 
        END as geocoding_pct
    FROM `{SOURCE_TABLE}`
    WHERE {PATIENT_ENTRY_DATE} = @processing_date
      AND {AREA_TYPE} = 'Urban'
    """
    
    df = execute_query(geocoding_query, [('processing_date', 'DATE', processing_date)])
    
    if len(df) == 0 or df.total_urban[0] == 0:
        logger.info("No urban patients found - geocoding check passed")
        return True
    
    geocoding_pct = df.geocoding_pct[0]
    total_urban = df.total_urban[0]
    geocoded_urban = df.geocoded_urban[0]
    
    logger.info(f"Geocoding quality: {geocoded_urban}/{total_urban} ({geocoding_pct*100:.1f}%)")
    
    if geocoding_pct < GEOCODING_THRESHOLD:
        logger.warning(f"Geocoding quality {geocoding_pct*100:.1f}% below threshold {GEOCODING_THRESHOLD*100:.1f}%")
        return False
    
    # Check streaming buffer
    try:
        buffer_query = f"""
        SELECT COUNT(*) as recent_inserts
        FROM `{SMART_CLUSTERS_TABLE}`
        WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), created_at, SECOND) < 120
        """
        buffer_df = execute_query(buffer_query)
        recent_inserts = buffer_df.recent_inserts[0] if len(buffer_df) > 0 else 0
        
        if recent_inserts > 0:
            logger.warning(f"Streaming buffer active: {recent_inserts} recent inserts")
            time.sleep(STREAMING_BUFFER_WAIT)
            logger.info(f"Waited {STREAMING_BUFFER_WAIT}s for streaming buffer")
    except Exception as e:
        logger.warning(f"Could not check streaming buffer: {str(e)}")
    
    logger.info("Data quality checks passed")
    return True

def mark_date_completed(processing_date, worker_id):
    """Mark date as completed"""
    execute_query(
        f"UPDATE `{PROJECT_ID}.{DATASET_ID}.smart_processing_status` SET status = 'COMPLETED', completed_at = @timestamp WHERE date = @date AND worker_id = @worker_id",
        [
            ('date', 'DATE', processing_date),
            ('worker_id', 'STRING', worker_id),
            ('timestamp', 'TIMESTAMP', datetime.now(timezone.utc))
        ]
    )

def mark_date_failed(processing_date, worker_id, error_message):
    """Mark date as failed with error details"""
    execute_query(
        f"UPDATE `{PROJECT_ID}.{DATASET_ID}.smart_processing_status` SET status = 'FAILED', completed_at = @timestamp WHERE date = @date AND worker_id = @worker_id",
        [
            ('date', 'DATE', processing_date),
            ('worker_id', 'STRING', worker_id),
            ('timestamp', 'TIMESTAMP', datetime.now(timezone.utc))
        ]
    )
    logger.error(f"Processing failed for {processing_date} by {worker_id}: {error_message}")

def execute_query(query, parameters=None):
    """Execute BigQuery with parameterization and error handling"""
    job_config = None
    if parameters:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(name, param_type, value)
                for name, param_type, value in parameters
            ]
        )
    
    result = client.query(query, job_config=job_config)
    return result.to_dataframe() if result.result() else None

def insert_rows_safe(table_name, rows):
    """Safe BigQuery insert with error handling"""
    errors = client.insert_rows_json(client.get_table(table_name), rows)
    if errors:
        logger.error(f"BigQuery insert failed for {table_name}: {errors}")
        raise Exception(f"Insert failed: {len(errors)} errors in {table_name}")

def validate_config():
    """Validate environment variables at startup"""
    required_vars = ['GCP_PROJECT_ID', 'DATASET_ID']
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")
    
    # Validate numeric parameters
    try:
        assert MIN_CLUSTER_SIZE >= 2, "MIN_CLUSTER_SIZE must be >= 2"
        assert MAX_CLUSTER_AGE_DAYS > 0, "MAX_CLUSTER_AGE_DAYS must be > 0"
        assert MAX_CLUSTER_RADIUS > 0, "MAX_CLUSTER_RADIUS must be > 0"
        logger.info("Configuration validation passed")
    except AssertionError as e:
        raise ValueError(f"Invalid configuration: {e}")

def smart_abc_clustering(processing_date):
    """Smart ABC clustering with merge detection"""
    logger.info("Running smart ABC clustering...")
    
    # Get rural patients for processing date
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
            t.{PATIENT_ENTRY_DATE} = DATE('{processing_date}')
            AND t.{AREA_TYPE} = 'Rural'
            AND t.{VILLAGE_NAME} IS NOT NULL
    )
    SELECT *
    FROM base_clusters
    WHERE cluster_count >= {MIN_CLUSTER_SIZE}
    """
    
    abc_df = client.query(abc_query).to_dataframe()
    
    if len(abc_df) == 0:
        logger.info("No ABC clusters found")
        return {'clusters': 0, 'expansions': 0}
    
    clusters_created = 0
    expansions_performed = 0
    
    # Group by village + syndrome
    for (state, district, subdistrict, village, syndrome), group in abc_df.groupby([
        STATE, DISTRICT, SUBDISTRICT, VILLAGE_NAME, CLINICAL_PRIMARY_SYNDROME
    ]):
        
        # Check for existing cluster to merge with
        existing_cluster = find_mergeable_abc_cluster(
            processing_date, state, district, subdistrict, village, syndrome
        )
        
        if existing_cluster:
            # Expand existing cluster
            expand_abc_cluster(existing_cluster, group, processing_date)
            expansions_performed += 1
        else:
            # Create new cluster
            create_new_abc_cluster(group, processing_date, village)
            clusters_created += 1
    
    logger.info(f"ABC: Created {clusters_created} clusters, expanded {expansions_performed}")
    return {'clusters': clusters_created, 'expansions': expansions_performed}

def find_mergeable_abc_cluster(processing_date, state, district, subdistrict, village, syndrome):
    """Find existing ABC cluster that can be merged with"""
    query = f"""
    SELECT 
        c.smart_cluster_id,
        c.original_creation_date,
        c.patient_count,
        c.expansion_count
    FROM `{SMART_CLUSTERS_TABLE}` c
    WHERE c.algorithm_type = 'ABC'
      AND c.village_name = @village
      AND c.primary_syndrome = @syndrome
      AND DATE_DIFF(@processing_date, c.original_creation_date, DAY) <= @max_age_days
    ORDER BY c.original_creation_date ASC
    LIMIT 1
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('village', 'STRING', village),
            bigquery.ScalarQueryParameter('syndrome', 'STRING', syndrome),
            bigquery.ScalarQueryParameter('processing_date', 'DATE', processing_date),
            bigquery.ScalarQueryParameter('max_age_days', 'INT64', MAX_CLUSTER_AGE_DAYS)
        ]
    )
    
    df = client.query(query, job_config=job_config).to_dataframe()
    return df.iloc[0].to_dict() if len(df) > 0 else None

def expand_abc_cluster(existing_cluster, new_patients, processing_date):
    """Expand existing ABC cluster with new patients"""
    cluster_id = existing_cluster['smart_cluster_id']
    
    # Get existing patients
    existing_query = f"""
    SELECT unique_id
    FROM `{SMART_ASSIGNMENTS_TABLE}`
    WHERE smart_cluster_id = @cluster_id
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('cluster_id', 'STRING', cluster_id)
        ]
    )
    
    existing_df = client.query(existing_query, job_config=job_config).to_dataframe()
    existing_patients = set(existing_df['unique_id']) if len(existing_df) > 0 else set()
    
    # Find new patients (remove overlaps)
    new_patient_ids = set(new_patients[UNIQUE_ID])
    overlap_patients = new_patient_ids & existing_patients
    truly_new_patients = new_patient_ids - existing_patients
    
    if len(truly_new_patients) == 0:
        logger.info(f"No new patients to add to {cluster_id}")
        return
    
    # Add new assignments
    assignments_to_insert = []
    for patient_id in truly_new_patients:
        assignments_to_insert.append({
            'assignment_id': str(uuid.uuid4()),
            'smart_cluster_id': cluster_id,
            'unique_id': patient_id,
            'assigned_at': datetime.now(timezone.utc).isoformat(),
            'addition_type': 'EXPANSION',
            'expansion_date': processing_date
        })
    
    insert_rows_safe(SMART_ASSIGNMENTS_TABLE, assignments_to_insert)
    
    # Update cluster
    update_query = f"""
    UPDATE `{SMART_CLUSTERS_TABLE}`
    SET 
        patient_count = patient_count + @new_patient_count,
        expansion_count = expansion_count + 1,
        input_date = @processing_date
    WHERE smart_cluster_id = @cluster_id
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('new_patient_count', 'INT64', len(truly_new_patients)),
            bigquery.ScalarQueryParameter('processing_date', 'DATE', processing_date),
            bigquery.ScalarQueryParameter('cluster_id', 'STRING', cluster_id)
        ]
    )
    
    client.query(update_query, job_config=job_config)
    
    # Log merge history
    merge_record = {
        'merge_id': f"ABC-EXP-{cluster_id}-{processing_date.replace('-', '')}",
        'target_cluster_id': cluster_id,
        'source_cluster_id': f"NEW-{processing_date}",
        'merge_reason': 'TIME_CONTINUITY',
        'cases_added': len(truly_new_patients),
        'overlap_cases_removed': len(overlap_patients),
        'performed_at': datetime.now(timezone.utc).isoformat()
    }
    insert_rows_safe(SMART_MERGE_HISTORY_TABLE, [merge_record])
    
    logger.info(f"Expanded {cluster_id}: +{len(truly_new_patients)} patients, -{len(overlap_patients)} overlaps")

def create_new_abc_cluster(patients_group, processing_date, village):
    """Create new ABC cluster"""
    cluster_id = f"SMART-ABC-{int(time.time())}-{uuid.uuid4().hex[:8]}"
    
    # Create cluster record
    cluster_record = {
        'smart_cluster_id': cluster_id,
        'algorithm_type': 'ABC',
        'input_date': processing_date,
        'original_creation_date': processing_date,
        'actual_cluster_radius': 0.0,
        'accept_status': 'Accepted',
        'patient_count': len(patients_group),
        'primary_syndrome': patients_group.iloc[0][CLINICAL_PRIMARY_SYNDROME],
        'centroid_lat': None,
        'centroid_lon': None,
        'village_name': village,
        'expansion_count': 0,
        'created_at': datetime.now(timezone.utc).isoformat()
    }
    
    insert_rows_safe(SMART_CLUSTERS_TABLE, [cluster_record])
    
    # Create assignments
    assignments_to_insert = []
    for _, patient in patients_group.iterrows():
        assignments_to_insert.append({
            'assignment_id': str(uuid.uuid4()),
            'smart_cluster_id': cluster_id,
            'unique_id': patient[UNIQUE_ID],
            'assigned_at': datetime.now(timezone.utc).isoformat(),
            'addition_type': 'ORIGINAL',
            'expansion_date': None
        })
    
    insert_rows_safe(SMART_ASSIGNMENTS_TABLE, assignments_to_insert)
    
    logger.info(f"Created new ABC cluster {cluster_id} with {len(patients_group)} patients")

def smart_gis_clustering(processing_date):
    """Smart GIS clustering with merge detection"""
    logger.info("Running smart GIS clustering...")
    
    from sklearn.cluster import DBSCAN
    from sklearn.metrics.pairwise import haversine_distances
    
    # Get urban patients for processing date
    gis_query = f"""
    SELECT 
        {UNIQUE_ID},
        {CLINICAL_PRIMARY_SYNDROME},
        {LATITUDE},
        {LONGITUDE}
    FROM `{SOURCE_TABLE}`
    WHERE 
        {PATIENT_ENTRY_DATE} = DATE(@processing_date)
        AND {AREA_TYPE} = 'Urban'
        AND {LATITUDE} BETWEEN 8 AND 37
        AND {LONGITUDE} BETWEEN 68 AND 97
        AND {LATITUDE} != 0.0
        AND {LONGITUDE} != 0.0
        AND (pat_street IS NOT NULL AND pat_street != '' OR villagename IS NOT NULL AND villagename != '')
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('processing_date', 'DATE', processing_date)
        ]
    )
    
    patients_df = client.query(gis_query, job_config=job_config).to_dataframe()
    
    if len(patients_df) == 0:
        logger.info("No GIS patients found")
        return {'clusters': 0, 'expansions': 0}
    
    clusters_created = 0
    expansions_performed = 0
    
    syndromes = patients_df[CLINICAL_PRIMARY_SYNDROME].unique()
    dbscan_assigned_patients = set()
    
    # DBSCAN clustering by syndrome
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
            
            # Calculate cluster radius
            cluster_coords = cluster_patients[[LATITUDE, LONGITUDE]].values
            centroid_coords = np.array([[centroid_lat, centroid_lon]])
            
            distances = haversine_distances(
                np.radians(cluster_coords),
                np.radians(centroid_coords)
            ) * 6371000
            
            cluster_radius = distances.max()
            
            if cluster_radius > MAX_CLUSTER_RADIUS:
                logger.info(f"Cluster radius {cluster_radius}m exceeds limit, skipping")
                continue
            
            # Check for existing cluster to merge with
            existing_cluster = find_mergeable_gis_cluster(
                processing_date, syndrome, centroid_lat, centroid_lon, cluster_radius
            )
            
            if existing_cluster:
                # Expand existing cluster
                expand_gis_cluster(existing_cluster, cluster_patients, processing_date, centroid_lat, centroid_lon)
                expansions_performed += 1
            else:
                # Create new cluster
                create_new_gis_cluster(cluster_patients, processing_date, syndrome, centroid_lat, centroid_lon, cluster_radius)
                clusters_created += 1
            
            # Mark patients as assigned
            for idx, patient in cluster_patients.iterrows():
                dbscan_assigned_patients.add(patient[UNIQUE_ID])
    
    # Handle exact coordinate clustering for unassigned patients
    unassigned_df = patients_df[~patients_df[UNIQUE_ID].isin(dbscan_assigned_patients)].copy()
    
    if len(unassigned_df) > 0:
        syndromes_unassigned = unassigned_df[CLINICAL_PRIMARY_SYNDROME].unique()
        
        for syndrome in syndromes_unassigned:
            syndrome_df = unassigned_df[unassigned_df[CLINICAL_PRIMARY_SYNDROME] == syndrome].copy()
            coord_groups = syndrome_df.groupby([LATITUDE, LONGITUDE])
            
            for (lat, lon), group in coord_groups:
                if len(group) >= MIN_CLUSTER_SIZE:
                    # Check for existing cluster to merge with
                    existing_cluster = find_mergeable_gis_cluster(
                        processing_date, syndrome, lat, lon, 0.0
                    )
                    
                    if existing_cluster:
                        expand_gis_cluster(existing_cluster, group, processing_date, lat, lon)
                        expansions_performed += 1
                    else:
                        create_new_gis_cluster(group, processing_date, syndrome, lat, lon, 0.0)
                        clusters_created += 1
    
    logger.info(f"GIS: Created {clusters_created} clusters, expanded {expansions_performed}")
    return {'clusters': clusters_created, 'expansions': expansions_performed}

def find_mergeable_gis_cluster(processing_date, syndrome, lat, lon, radius):
    """Find existing GIS cluster that can be merged with"""
    query = f"""
    SELECT 
        c.smart_cluster_id,
        c.original_creation_date,
        c.patient_count,
        c.expansion_count,
        c.centroid_lat,
        c.centroid_lon,
        c.actual_cluster_radius
    FROM `{SMART_CLUSTERS_TABLE}` c
    WHERE c.algorithm_type = 'GIS'
      AND c.primary_syndrome = @syndrome
      AND DATE_DIFF(@processing_date, c.original_creation_date, DAY) <= @max_age_days
      AND ST_DISTANCE(
          ST_GEOGPOINT(c.centroid_lon, c.centroid_lat),
          ST_GEOGPOINT(@lon, @lat)
      ) <= @epsilon_m
    ORDER BY c.original_creation_date ASC
    LIMIT 1
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('syndrome', 'STRING', syndrome),
            bigquery.ScalarQueryParameter('processing_date', 'DATE', processing_date),
            bigquery.ScalarQueryParameter('max_age_days', 'INT64', MAX_CLUSTER_AGE_DAYS),
            bigquery.ScalarQueryParameter('lat', 'FLOAT64', lat),
            bigquery.ScalarQueryParameter('lon', 'FLOAT64', lon),
            bigquery.ScalarQueryParameter('epsilon_m', 'INT64', DBSCAN_EPSILON_M)
        ]
    )
    
    df = client.query(query, job_config=job_config).to_dataframe()
    return df.iloc[0].to_dict() if len(df) > 0 else None

def expand_gis_cluster(existing_cluster, new_patients, processing_date, new_centroid_lat, new_centroid_lon):
    """Expand existing GIS cluster with new patients"""
    cluster_id = existing_cluster['smart_cluster_id']
    
    # Get existing patients with coordinates
    existing_query = f"""
    SELECT p.unique_id, p.{LATITUDE}, p.{LONGITUDE}
    FROM `{SMART_ASSIGNMENTS_TABLE}` a
    JOIN `{SOURCE_TABLE}` p ON a.unique_id = p.unique_id
    WHERE a.smart_cluster_id = @cluster_id
      AND p.{LATITUDE} IS NOT NULL AND p.{LONGITUDE} IS NOT NULL
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('cluster_id', 'STRING', cluster_id)
        ]
    )
    
    existing_df = client.query(existing_query, job_config=job_config).to_dataframe()
    existing_patients = set(existing_df['unique_id']) if len(existing_df) > 0 else set()
    
    # Find new patients (remove overlaps)
    new_patient_ids = set(new_patients[UNIQUE_ID])
    overlap_patients = new_patient_ids & existing_patients
    truly_new_patients = new_patient_ids - existing_patients
    
    if len(truly_new_patients) == 0:
        logger.info(f"No new patients to add to GIS cluster {cluster_id}")
        return
    
    # Check if expansion would exceed 1000m radius limit
    from sklearn.metrics.pairwise import haversine_distances
    all_coords = []
    
    # Add existing patient coordinates
    for _, patient in existing_df.iterrows():
        all_coords.append([patient[LATITUDE], patient[LONGITUDE]])
    
    # Add new patient coordinates
    for _, patient in new_patients.iterrows():
        if patient[UNIQUE_ID] in truly_new_patients:
            all_coords.append([patient[LATITUDE], patient[LONGITUDE]])
    
    if len(all_coords) > 0:
        # Calculate new centroid
        old_count = existing_cluster['patient_count']
        new_count = len(truly_new_patients)
        total_count = old_count + new_count
        
        old_lat = existing_cluster['centroid_lat']
        old_lon = existing_cluster['centroid_lon']
        
        weighted_lat = (old_lat * old_count + new_centroid_lat * new_count) / total_count
        weighted_lon = (old_lon * old_count + new_centroid_lon * new_count) / total_count
        
        # Calculate potential new radius
        all_coords_array = np.array(all_coords)
        centroid_coords = np.array([[weighted_lat, weighted_lon]])
        
        distances = haversine_distances(
            np.radians(all_coords_array),
            np.radians(centroid_coords)
        ) * 6371000
        
        potential_radius = float(distances.max())
        
        if potential_radius > 1000:
            logger.info(f"Expansion blocked: radius would be {potential_radius:.1f}m (>1000m limit)")
            return
    
    # Use already calculated values from radius check
    new_radius = potential_radius
    
    # Add new assignments
    assignments_to_insert = []
    for patient_id in truly_new_patients:
        assignments_to_insert.append({
            'assignment_id': str(uuid.uuid4()),
            'smart_cluster_id': cluster_id,
            'unique_id': patient_id,
            'assigned_at': datetime.now(timezone.utc).isoformat(),
            'addition_type': 'EXPANSION',
            'expansion_date': processing_date
        })
    
    errors = client.insert_rows_json(client.get_table(SMART_ASSIGNMENTS_TABLE), assignments_to_insert)
    if errors:
        logger.error(f"Failed to insert GIS assignments: {errors}")
        raise Exception(f"BigQuery insert failed: {len(errors)} assignment errors")
    
    # Update cluster with new centroid, count, and radius
    update_query = f"""
    UPDATE `{SMART_CLUSTERS_TABLE}`
    SET 
        patient_count = patient_count + @new_patient_count,
        expansion_count = expansion_count + 1,
        input_date = @processing_date,
        centroid_lat = @new_lat,
        centroid_lon = @new_lon,
        actual_cluster_radius = @new_radius
    WHERE smart_cluster_id = @cluster_id
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('new_patient_count', 'INT64', len(truly_new_patients)),
            bigquery.ScalarQueryParameter('processing_date', 'DATE', processing_date),
            bigquery.ScalarQueryParameter('new_lat', 'FLOAT64', weighted_lat),
            bigquery.ScalarQueryParameter('new_lon', 'FLOAT64', weighted_lon),
            bigquery.ScalarQueryParameter('new_radius', 'FLOAT64', new_radius),
            bigquery.ScalarQueryParameter('cluster_id', 'STRING', cluster_id)
        ]
    )
    
    client.query(update_query, job_config=job_config)
    
    # Log merge history
    merge_record = {
        'merge_id': f"GIS-EXP-{cluster_id}-{processing_date.replace('-', '')}",
        'target_cluster_id': cluster_id,
        'source_cluster_id': f"NEW-{processing_date}",
        'merge_reason': 'GEOGRAPHIC_PROXIMITY',
        'cases_added': len(truly_new_patients),
        'overlap_cases_removed': len(overlap_patients),
        'performed_at': datetime.now(timezone.utc).isoformat()
    }
    
    errors = client.insert_rows_json(client.get_table(SMART_MERGE_HISTORY_TABLE), [merge_record])
    if errors:
        logger.error(f"Failed to insert GIS merge history: {errors}")
        raise Exception(f"BigQuery merge history insert failed: {errors}")
    
    logger.info(f"Expanded GIS {cluster_id}: +{len(truly_new_patients)} patients, -{len(overlap_patients)} overlaps, radius: {new_radius:.1f}m")

def create_new_gis_cluster(patients_group, processing_date, syndrome, centroid_lat, centroid_lon, radius):
    """Create new GIS cluster"""
    cluster_id = f"SMART-GIS-{int(time.time())}-{uuid.uuid4().hex[:8]}"
    
    # Create cluster record
    cluster_record = {
        'smart_cluster_id': cluster_id,
        'algorithm_type': 'GIS',
        'input_date': processing_date,
        'original_creation_date': processing_date,
        'actual_cluster_radius': float(radius),
        'accept_status': 'Accepted' if radius < AUTO_ACCEPT_RADIUS_THRESHOLD else 'Pending',
        'patient_count': len(patients_group),
        'primary_syndrome': syndrome,
        'centroid_lat': float(centroid_lat),
        'centroid_lon': float(centroid_lon),
        'village_name': None,
        'expansion_count': 0,
        'created_at': datetime.now(timezone.utc).isoformat()
    }
    
    errors = client.insert_rows_json(client.get_table(SMART_CLUSTERS_TABLE), [cluster_record])
    if errors:
        logger.error(f"Failed to insert GIS cluster: {errors}")
        raise Exception(f"BigQuery cluster insert failed: {errors}")
    
    # Create assignments
    assignments_to_insert = []
    for _, patient in patients_group.iterrows():
        assignments_to_insert.append({
            'assignment_id': str(uuid.uuid4()),
            'smart_cluster_id': cluster_id,
            'unique_id': patient[UNIQUE_ID],
            'assigned_at': datetime.now(timezone.utc).isoformat(),
            'addition_type': 'ORIGINAL',
            'expansion_date': None
        })
    
    errors = client.insert_rows_json(client.get_table(SMART_ASSIGNMENTS_TABLE), assignments_to_insert)
    if errors:
        logger.error(f"Failed to insert new GIS cluster assignments: {errors}")
        raise Exception(f"BigQuery assignment insert failed: {len(errors)} errors")
    
    logger.info(f"Created new GIS cluster {cluster_id} with {len(patients_group)} patients")

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        'service': 'Smart Clustering Engine',
        'version': '1.0.0',
        'status': 'running',
        'endpoints': {
            'smart-process': '/smart-process (POST)',
            'smart-status': '/smart-status (GET)',
            'smart-config': '/smart-config (GET)'
        }
    })

@app.route('/health', methods=['GET'])
def health():
    """Enhanced health check with system metrics"""
    try:
        # Test BigQuery connection
        client.query("SELECT 1").result()
        
        # Get processing status
        status_query = f"""
        SELECT 
            status,
            COUNT(*) as count,
            MAX(started_at) as last_activity
        FROM `{PROJECT_ID}.{DATASET_ID}.smart_processing_status`
        WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        GROUP BY status
        """
        
        try:
            status_df = client.query(status_query).to_dataframe()
            status_summary = {row.status: int(row.count) for _, row in status_df.iterrows()}
        except:
            status_summary = {}
        
        return jsonify({
            'status': 'healthy',
            'bigquery': 'connected',
            'last_24h_processing': status_summary,
            'config': {
                'max_cluster_age_days': MAX_CLUSTER_AGE_DAYS,
                'max_cluster_radius': MAX_CLUSTER_RADIUS,
                'min_cluster_size': MIN_CLUSTER_SIZE
            },
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 500



@app.route('/smart-process', methods=['POST'])
def smart_process():
    """Process next date with smart clustering"""
    try:
        processing_date, worker_id = claim_next_date()
        if not processing_date:
            return jsonify({
                'success': True,
                'message': 'All dates processed or claimed by other workers',
                'date_processed': None
            })
        
        logger.info(f"Worker {worker_id} processing date: {processing_date}")
        
        try:
            # Check data quality before processing
            if not check_data_quality(processing_date):
                mark_date_failed(processing_date, worker_id, "Data quality checks failed")
                return jsonify({
                    'success': False,
                    'message': 'Data quality checks failed',
                    'date_processed': processing_date,
                    'worker_id': worker_id
                })
            
            # ABC Clustering
            abc_result = smart_abc_clustering(processing_date)
            
            # GIS Clustering
            gis_result = smart_gis_clustering(processing_date)
            
            # Mark as completed
            mark_date_completed(processing_date, worker_id)
            
            return jsonify({
                'success': True,
                'message': 'Smart processing complete',
                'date_processed': processing_date,
                'worker_id': worker_id,
                'abc_clusters': abc_result['clusters'],
                'abc_expansions': abc_result['expansions'],
                'gis_clusters': gis_result['clusters'],
                'gis_expansions': gis_result['expansions'],
                'cluster_limits': {
                    'max_cluster_age_days': MAX_CLUSTER_AGE_DAYS,
                    'dbscan_epsilon_m': DBSCAN_EPSILON_M,
                    'max_cluster_radius_m': MAX_CLUSTER_RADIUS,
                    'min_cluster_size': MIN_CLUSTER_SIZE,
                    'auto_accept_radius_m': AUTO_ACCEPT_RADIUS_THRESHOLD
                },
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
            
        except Exception as processing_error:
            # Mark as failed with error details
            mark_date_failed(processing_date, worker_id, str(processing_error))
            raise processing_error
        
    except Exception as e:
        logger.error(f"Error in smart processing: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/smart-status', methods=['GET'])
def smart_status():
    """Get smart clustering status"""
    try:
        query = f"""
        SELECT 
            COUNT(DISTINCT input_date) as dates_processed,
            MAX(input_date) as last_processed_date,
            COUNT(*) as total_clusters,
            COUNTIF(algorithm_type = 'ABC') as abc_clusters,
            COUNTIF(algorithm_type = 'GIS') as gis_clusters,
            COUNTIF(algorithm_type = 'GIS' AND accept_status = 'Accepted') as gis_accepted,
            COUNTIF(algorithm_type = 'GIS' AND accept_status = 'Pending') as gis_pending,
            SUM(expansion_count) as total_expansions,
            SUM(patient_count) as total_patients
        FROM `{SMART_CLUSTERS_TABLE}`
        """
        df = client.query(query).to_dataframe()
        
        return jsonify({
            'dates_processed': int(df.dates_processed[0]) if len(df) > 0 else 0,
            'last_processed_date': df.last_processed_date[0].isoformat() if len(df) > 0 and pd.notna(df.last_processed_date[0]) else None,
            'total_clusters': int(df.total_clusters[0]) if len(df) > 0 else 0,
            'abc_clusters': int(df.abc_clusters[0]) if len(df) > 0 else 0,
            'gis_clusters': int(df.gis_clusters[0]) if len(df) > 0 else 0,
            'gis_accepted': int(df.gis_accepted[0]) if len(df) > 0 else 0,
            'gis_pending': int(df.gis_pending[0]) if len(df) > 0 else 0,
            'total_expansions': int(df.total_expansions[0]) if len(df) > 0 else 0,
            'total_patients': int(df.total_patients[0]) if len(df) > 0 else 0,
            'cluster_limits': {
                'max_cluster_age_days': MAX_CLUSTER_AGE_DAYS,
                'dbscan_epsilon_m': DBSCAN_EPSILON_M,
                'max_cluster_radius_m': MAX_CLUSTER_RADIUS,
                'min_cluster_size': MIN_CLUSTER_SIZE,
                'auto_accept_radius_m': AUTO_ACCEPT_RADIUS_THRESHOLD
            },
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/smart-preflight', methods=['GET'])
def smart_preflight():
    """Standalone data quality check with pending clusters validation"""
    try:
        # Check for pending clusters
        pending_query = f"""
        SELECT COUNT(*) as pending_count
        FROM `{SMART_CLUSTERS_TABLE}`
        WHERE algorithm_type = 'GIS' AND accept_status = 'Pending'
        """
        
        try:
            pending_df = client.query(pending_query).to_dataframe()
            pending_count = int(pending_df.pending_count[0]) if len(pending_df) > 0 else 0
        except:
            pending_count = 0
        
        # Get next unprocessed date
        query = f"""
        WITH source_dates AS (
            SELECT DISTINCT {PATIENT_ENTRY_DATE} as date
            FROM `{SOURCE_TABLE}`
        ),
        processed_dates AS (
            SELECT DISTINCT date
            FROM `{PROJECT_ID}.{DATASET_ID}.smart_processing_status`
            WHERE status IN ('IN_PROGRESS', 'COMPLETED')
        )
        SELECT s.date
        FROM source_dates s
        LEFT JOIN processed_dates p ON s.date = p.date
        WHERE p.date IS NULL
        ORDER BY s.date ASC
        LIMIT 1
        """
        
        df = client.query(query).to_dataframe()
        if len(df) == 0:
            return jsonify({
                'success': True, 
                'message': 'All dates processed', 
                'overall_passed': pending_count == 0,
                'pending_clusters': pending_count
            })
        
        test_date = df.date[0].strftime('%Y-%m-%d')
        quality_passed = check_data_quality(test_date)
        overall_passed = quality_passed and pending_count == 0
        
        return jsonify({
            'success': True,
            'test_date': test_date,
            'overall_passed': overall_passed,
            'data_quality_passed': quality_passed,
            'pending_clusters': pending_count,
            'geocoding_threshold': GEOCODING_THRESHOLD * 100,
            'message': 'Preflight check completed'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/smart-test', methods=['POST'])
def smart_test():
    """Test clustering without writing to BigQuery"""
    try:
        query = f"""
        WITH source_dates AS (
            SELECT DISTINCT {PATIENT_ENTRY_DATE} as date
            FROM `{SOURCE_TABLE}`
        ),
        processed_dates AS (
            SELECT DISTINCT date
            FROM `{PROJECT_ID}.{DATASET_ID}.smart_processing_status`
            WHERE status IN ('IN_PROGRESS', 'COMPLETED')
        )
        SELECT s.date
        FROM source_dates s
        LEFT JOIN processed_dates p ON s.date = p.date
        WHERE p.date IS NULL
        ORDER BY s.date ASC
        LIMIT 1
        """
        
        df = client.query(query).to_dataframe()
        if len(df) == 0:
            return jsonify({'success': False, 'message': 'No unprocessed dates available for testing'})
        
        test_date = df.date[0].strftime('%Y-%m-%d')
        quality_passed = check_data_quality(test_date)
        
        return jsonify({
            'success': True,
            'test_date': test_date,
            'data_quality_passed': quality_passed,
            'message': 'Test completed successfully'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/smart-init', methods=['POST'])
def smart_init():
    """Initialize smart clustering tables"""
    try:
        validate_config()
        create_smart_tables()
        return jsonify({
            'success': True,
            'message': 'Smart clustering tables created successfully'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/smart-cluster', methods=['POST'])
def smart_cluster():
    """Process single date with smart clustering"""
    try:
        return smart_process()
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/smart-batch', methods=['POST'])
def smart_batch():
    """Process multiple dates"""
    try:
        max_dates = request.json.get('max_dates', 5) if request.json else 5
        
        results = []
        for i in range(max_dates):
            result = smart_process()
            if not result.get_json().get('success') or not result.get_json().get('date_processed'):
                break
            results.append(result.get_json())
        
        return jsonify({
            'success': True,
            'dates_processed': len(results),
            'results': results
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/smart-config', methods=['GET', 'POST'])
def smart_config():
    """Get or update smart clustering configuration"""
    if request.method == 'GET':
        return jsonify({
            'parameters': {
                'time_window_days': TIME_WINDOW_DAYS,
                'max_cluster_age_days': MAX_CLUSTER_AGE_DAYS,
                'max_cluster_radius': MAX_CLUSTER_RADIUS,
                'min_cluster_size': MIN_CLUSTER_SIZE,
                'dbscan_epsilon_m': DBSCAN_EPSILON_M,
                'geocoding_threshold': GEOCODING_THRESHOLD,
                'auto_accept_radius': AUTO_ACCEPT_RADIUS_THRESHOLD
            },
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
    
    elif request.method == 'POST':
        return jsonify({
            'success': False,
            'message': 'Configuration updates require service restart'
        }), 400

@app.route('/smart-clusters', methods=['GET'])
def smart_clusters():
    """Get smart clusters with optional date filtering"""
    try:
        days = request.args.get('days', type=int)
        
        if days:
            query = f"""
            WITH recent_dates AS (
                SELECT DISTINCT original_creation_date
                FROM `{SMART_CLUSTERS_TABLE}`
                ORDER BY original_creation_date DESC
                LIMIT {days}
            )
            SELECT 
                c.smart_cluster_id,
                c.algorithm_type,
                c.input_date,
                c.original_creation_date,
                c.actual_cluster_radius,
                c.accept_status,
                c.patient_count,
                c.primary_syndrome,
                c.centroid_lat,
                c.centroid_lon,
                c.village_name,
                c.expansion_count,
                c.created_at
            FROM `{SMART_CLUSTERS_TABLE}` c
            JOIN recent_dates r ON c.original_creation_date = r.original_creation_date
            ORDER BY c.created_at DESC
            """
            
            df = client.query(query.format(days=days)).to_dataframe()
        else:
            query = f"""
            WITH cluster_sites AS (
                SELECT 
                    a.smart_cluster_id,
                    p.site_code,
                    COUNT(*) as site_count,
                    ROW_NUMBER() OVER (PARTITION BY a.smart_cluster_id ORDER BY COUNT(*) DESC) as rn
                FROM `{SMART_ASSIGNMENTS_TABLE}` a
                JOIN `{SOURCE_TABLE}` p ON a.unique_id = p.unique_id
                WHERE p.site_code IS NOT NULL
                GROUP BY a.smart_cluster_id, p.site_code
            )
            SELECT 
                c.smart_cluster_id,
                c.algorithm_type,
                c.input_date,
                c.original_creation_date,
                c.actual_cluster_radius,
                c.accept_status,
                c.patient_count,
                c.primary_syndrome,
                c.centroid_lat,
                c.centroid_lon,
                c.village_name,
                c.expansion_count,
                c.created_at,
                cs.site_code as most_common_site_code
            FROM `{SMART_CLUSTERS_TABLE}` c
            LEFT JOIN cluster_sites cs ON c.smart_cluster_id = cs.smart_cluster_id AND cs.rn = 1
            ORDER BY c.created_at DESC
            """
            
            df = client.query(query).to_dataframe()
        
        df = client.query(query).to_dataframe()
        
        if len(df) == 0:
            return jsonify({
                'success': True,
                'clusters': [],
                'total_count': 0
            })
        
        clusters = []
        for _, row in df.iterrows():
            clusters.append({
                'smart_cluster_id': row['smart_cluster_id'],
                'algorithm_type': row['algorithm_type'],
                'input_date': row['input_date'].isoformat() if pd.notna(row['input_date']) else None,
                'original_creation_date': row['original_creation_date'].isoformat() if pd.notna(row['original_creation_date']) else None,
                'actual_cluster_radius': float(row['actual_cluster_radius']) if pd.notna(row['actual_cluster_radius']) else None,
                'accept_status': row['accept_status'],
                'patient_count': int(row['patient_count']),
                'primary_syndrome': row['primary_syndrome'],
                'centroid_lat': float(row['centroid_lat']) if pd.notna(row['centroid_lat']) else None,
                'centroid_lon': float(row['centroid_lon']) if pd.notna(row['centroid_lon']) else None,
                'village_name': row['village_name'] if pd.notna(row['village_name']) else None,
                'expansion_count': int(row['expansion_count']),
                'created_at': row['created_at'].isoformat() if pd.notna(row['created_at']) else None,
                'most_common_site_code': row['most_common_site_code'] if pd.notna(row['most_common_site_code']) else None
            })
        
        return jsonify({
            'success': True,
            'clusters': clusters,
            'total_count': len(clusters)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/smart-cluster-patients', methods=['GET'])
def smart_cluster_patients():
    """Get patients in a specific cluster"""
    try:
        cluster_id = request.args.get('cluster_id')
        if not cluster_id:
            return jsonify({'success': False, 'error': 'cluster_id parameter required'}), 400
        
        query = f"""
        SELECT 
            p.unique_id,
            p.patient_entry_date,
            p.patient_name,
            p.pat_age,
            p.pat_sex,
            p.villagename,
            p.statename,
            p.districtname,
            p.clini_primary_syn,
            p.latitude,
            p.longitude,
            p.site_code,
            a.addition_type,
            a.expansion_date,
            a.assigned_at
        FROM `{SMART_ASSIGNMENTS_TABLE}` a
        JOIN `{SOURCE_TABLE}` p ON a.unique_id = p.unique_id
        WHERE a.smart_cluster_id = @cluster_id
        ORDER BY a.assigned_at ASC
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('cluster_id', 'STRING', cluster_id)
            ]
        )
        
        df = client.query(query, job_config=job_config).to_dataframe()
        
        if len(df) == 0:
            return jsonify({
                'success': True,
                'patients': [],
                'patient_count': 0
            })
        
        patients = []
        for _, row in df.iterrows():
            patients.append({
                'unique_id': row['unique_id'],
                'patient_entry_date': row['patient_entry_date'].isoformat() if pd.notna(row['patient_entry_date']) else None,
                'patient_name': row['patient_name'] if pd.notna(row['patient_name']) else None,
                'pat_age': int(row['pat_age']) if pd.notna(row['pat_age']) else None,
                'pat_sex': row['pat_sex'] if pd.notna(row['pat_sex']) else None,
                'villagename': row['villagename'] if pd.notna(row['villagename']) else None,
                'statename': row['statename'] if pd.notna(row['statename']) else None,
                'districtname': row['districtname'] if pd.notna(row['districtname']) else None,
                'clini_primary_syn': row['clini_primary_syn'] if pd.notna(row['clini_primary_syn']) else None,
                'latitude': float(row['latitude']) if pd.notna(row['latitude']) else None,
                'longitude': float(row['longitude']) if pd.notna(row['longitude']) else None,
                'site_code': row['site_code'] if pd.notna(row['site_code']) else None,
                'addition_type': row['addition_type'],
                'expansion_date': row['expansion_date'].isoformat() if pd.notna(row['expansion_date']) else None,
                'assigned_at': row['assigned_at'].isoformat() if pd.notna(row['assigned_at']) else None
            })
        
        return jsonify({
            'success': True,
            'patients': patients,
            'patient_count': len(patients),
            'cluster_id': cluster_id
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/maps-api-key', methods=['GET'])
def maps_api_key():
    """Get Google Maps API key from Secret Manager"""
    try:
        from google.cloud import secretmanager
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{PROJECT_ID}/secrets/google_map_api_key/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_path})
        api_key = response.payload.data.decode("UTF-8")
        return jsonify({'success': True, 'api_key': api_key})
    except Exception as e:
        logger.error(f"Failed to get Maps API key: {str(e)}")
        return jsonify({'success': False, 'error': f'Secret Manager error: {str(e)}'}), 500

@app.route('/accept-cluster', methods=['POST'])
def accept_cluster():
    """Accept a pending cluster"""
    try:
        data = request.get_json()
        cluster_id = data.get('cluster_id')
        
        if not cluster_id:
            return jsonify({'success': False, 'error': 'cluster_id required'}), 400
        
        # Update cluster status to Accepted
        update_query = f"""
        UPDATE `{SMART_CLUSTERS_TABLE}`
        SET accept_status = 'Accepted'
        WHERE smart_cluster_id = @cluster_id
          AND accept_status = 'Pending'
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('cluster_id', 'STRING', cluster_id)
            ]
        )
        
        result = client.query(update_query, job_config=job_config)
        
        return jsonify({
            'success': True,
            'message': f'Cluster {cluster_id} accepted successfully',
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/reject-cluster', methods=['POST'])
def reject_cluster():
    """Reject and delete a pending cluster"""
    try:
        data = request.get_json()
        cluster_id = data.get('cluster_id')
        
        if not cluster_id:
            return jsonify({'success': False, 'error': 'cluster_id required'}), 400
        
        # Delete cluster assignments first
        delete_assignments_query = f"""
        DELETE FROM `{SMART_ASSIGNMENTS_TABLE}`
        WHERE smart_cluster_id = @cluster_id
        """
        
        # Delete cluster record
        delete_cluster_query = f"""
        DELETE FROM `{SMART_CLUSTERS_TABLE}`
        WHERE smart_cluster_id = @cluster_id
          AND accept_status = 'Pending'
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('cluster_id', 'STRING', cluster_id)
            ]
        )
        
        client.query(delete_assignments_query, job_config=job_config)
        client.query(delete_cluster_query, job_config=job_config)
        
        return jsonify({
            'success': True,
            'message': f'Cluster {cluster_id} rejected and deleted successfully',
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/smart-truncate', methods=['POST'])
def smart_truncate():
    """Truncate all smart clustering tables - WARNING: Deletes all data"""
    try:
        # Truncate all smart clustering tables
        tables_to_truncate = [
            SMART_CLUSTERS_TABLE,
            SMART_ASSIGNMENTS_TABLE,
            SMART_MERGE_HISTORY_TABLE,
            f"{PROJECT_ID}.{DATASET_ID}.smart_processing_status"
        ]
        
        truncated_tables = []
        for table_name in tables_to_truncate:
            try:
                client.query(f"TRUNCATE TABLE `{table_name}`")
                truncated_tables.append(table_name.split('.')[-1])
                logger.info(f"Truncated table: {table_name}")
            except Exception as e:
                logger.warning(f"Could not truncate {table_name}: {str(e)}")
        
        return jsonify({
            'success': True,
            'message': 'Smart clustering tables truncated',
            'truncated_tables': truncated_tables,
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    # Validate configuration at startup
    validate_config()
    
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)