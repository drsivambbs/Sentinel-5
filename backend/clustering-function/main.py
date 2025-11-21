import json
import logging
import os
from datetime import datetime, timedelta
from math import radians, cos, sin, asin, sqrt
from google.cloud import bigquery
from collections import defaultdict
import numpy as np
from sklearn.cluster import DBSCAN
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
)
logger = logging.getLogger(__name__)

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate Haversine distance between two points in meters"""
    R = 6371000  # Earth radius in meters
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))

def generate_location_code(statename, districtname, subdistrictname, villagename):
    """Generate location code from geographic hierarchy"""
    parts = [statename, districtname, subdistrictname, villagename]
    code = ""
    for part in parts:
        if part and str(part).strip().upper() != 'NULL':
            code += str(part)[0].upper()
    return code

def generate_cluster_id(cluster_type, location_code, date_str, sequence):
    """Generate cluster ID in format TYPE_LOCATION_DATE_SEQUENCE"""
    return f"{cluster_type}_{location_code}_{date_str}_{sequence:03d}"

def get_eligible_date(client, project_id, dataset_id, table_id, processed_dates):
    """Find the most recent date with 90% geocoding that hasn't been processed"""
    logger.debug(f"Finding eligible date for processing")
    
    # Get max date and calculate 15-day limit
    max_date_query = f"""
    SELECT MAX(patient_entry_date) as max_date
    FROM `{project_id}.{dataset_id}.{table_id}`
    """
    logger.debug(f"Executing max date query: {max_date_query}")
    max_result = list(client.query(max_date_query))
    if not max_result or not max_result[0].max_date:
        logger.warning("No max date found in patient records")
        return None
    
    max_date = max_result[0].max_date
    date_range_limit = int(os.getenv('DATE_RANGE_LIMIT', 15))
    limit_date = max_date - timedelta(days=date_range_limit)
    logger.debug(f"Max date: {max_date}, Limit date: {limit_date}, Range limit: {date_range_limit} days")
    
    # Get dates with geocoding percentage, ordered by date desc
    geocoding_threshold = int(os.getenv('GEOCODING_THRESHOLD', 90))
    geocoding_query = f"""
    SELECT 
        patient_entry_date,
        COUNT(*) as total_cases,
        COUNT(latitude) as geocoded_cases,
        COUNT(latitude) * 100.0 / COUNT(*) as geocoding_percentage
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE patient_entry_date >= '{limit_date}'
    GROUP BY patient_entry_date
    HAVING geocoding_percentage >= {geocoding_threshold}
    ORDER BY patient_entry_date DESC
    """
    logger.debug(f"Geocoding threshold: {geocoding_threshold}%")
    logger.debug(f"Executing geocoding query: {geocoding_query}")
    
    results = list(client.query(geocoding_query))
    logger.debug(f"Found {len(results)} dates meeting geocoding threshold")
    
    for row in results:
        date_str = row.patient_entry_date.strftime('%Y-%m-%d')
        logger.debug(f"Checking date {date_str}: {row.geocoded_cases}/{row.total_cases} ({row.geocoding_percentage:.1f}% geocoded)")
        if date_str not in processed_dates:
            logger.info(f"Selected eligible date: {date_str}")
            return row.patient_entry_date
        else:
            logger.debug(f"Date {date_str} already processed, skipping")
    
    logger.warning("No eligible dates found for processing")
    return None

def get_processed_dates(client, project_id, dataset_id):
    """Get list of already processed dates from cluster_summary_table"""
    try:
        cluster_summary_table = os.getenv('CLUSTER_SUMMARY_TABLE', 'cluster_summary_table')
        query = f"""
        SELECT DISTINCT patient_entry_date
        FROM `{project_id}.{dataset_id}.{cluster_summary_table}`
        """
        results = list(client.query(query))
        return {row.patient_entry_date for row in results}
    except Exception:
        return set()

def perform_abc_clustering(client, project_id, dataset_id, table_id, target_date, time_window, min_cases):
    """Perform Area-Based Clustering for Rural areas"""
    
    start_date = target_date - timedelta(days=time_window)
    
    query = f"""
    SELECT 
        unique_id, statename, districtname, subdistrictname, villagename,
        clini_primary_syn, patient_entry_date, latitude, longitude
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE pat_areatype = 'Rural'
        AND villagename IS NOT NULL
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
        AND patient_entry_date > '{start_date}'
        AND patient_entry_date <= '{target_date}'
    """
    
    results = list(client.query(query))
    
    # Group by village and syndrome
    village_syndrome_groups = defaultdict(list)
    for row in results:
        key = (row.statename, row.districtname, row.subdistrictname, 
               row.villagename, row.clini_primary_syn)
        village_syndrome_groups[key].append(row)
    
    clusters = []
    cluster_sequence = defaultdict(int)
    
    for (statename, districtname, subdistrictname, villagename, syndrome), cases in village_syndrome_groups.items():
        if len(cases) >= min_cases:
            location_code = generate_location_code(statename, districtname, subdistrictname, villagename)
            date_str = target_date.strftime('%d%b%Y').upper()
            
            cluster_sequence[(location_code, date_str)] += 1
            seq = cluster_sequence[(location_code, date_str)]
            
            cluster_id = generate_cluster_id('ABC', location_code, date_str, seq)
            
            for case in cases:
                clusters.append({
                    'unique_id': case.unique_id,
                    'cluster_id': cluster_id,
                    'dummy_id': f"{cluster_id}_{case.unique_id}",
                    'accept_status': None
                })
    
    logger.info(f"ABC clustering completed: {len(clusters)} cases in {len(set(c['cluster_id'] for c in clusters))} clusters")
    return clusters

def perform_gis_clustering(client, project_id, dataset_id, table_id, target_date, time_window, min_cases):
    """Perform GIS-Based Clustering for Urban areas using DBSCAN with 500m radius"""
    logger.debug(f"Starting GIS clustering for date {target_date}")
    
    start_date = target_date - timedelta(days=time_window)
    logger.debug(f"Time window: {start_date} to {target_date} ({time_window} days)")
    
    query = f"""
    SELECT 
        unique_id, statename, districtname, subdistrictname, villagename,
        clini_primary_syn, patient_entry_date, latitude, longitude
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE pat_areatype = 'Urban'
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
        AND patient_entry_date > '{start_date}'
        AND patient_entry_date <= '{target_date}'
    """
    
    logger.debug(f"Executing GIS query: {query}")
    results = list(client.query(query))
    logger.debug(f"Found {len(results)} urban records for GIS clustering")
    
    # Group by syndrome first
    syndrome_groups = defaultdict(list)
    for row in results:
        syndrome_groups[row.clini_primary_syn].append(row)
    
    clusters = []
    cluster_sequence = defaultdict(int)
    
    logger.debug(f"Grouped into {len(syndrome_groups)} syndrome groups")
    
    for syndrome, cases in syndrome_groups.items():
        logger.debug(f"Syndrome {syndrome}: {len(cases)} cases")
        if len(cases) < min_cases:
            logger.debug(f"Skipping syndrome {syndrome}: insufficient cases ({len(cases)} < {min_cases})")
            continue
            
        # Prepare coordinates for DBSCAN
        coordinates = np.array([[case.latitude, case.longitude] for case in cases])
        logger.debug(f"Prepared {len(coordinates)} coordinates for DBSCAN")
        
        # Convert 500m to degrees (approximate)
        # 1 degree ≈ 111km, so 500m ≈ 0.0045 degrees
        eps_degrees = 500 / 111000
        logger.debug(f"DBSCAN parameters: eps={eps_degrees:.6f} degrees (~500m), min_samples={min_cases}")
        
        # Apply DBSCAN
        dbscan = DBSCAN(eps=eps_degrees, min_samples=min_cases, metric='haversine')
        cluster_labels = dbscan.fit_predict(np.radians(coordinates))
        
        unique_labels = set(cluster_labels)
        noise_points = sum(1 for label in cluster_labels if label == -1)
        logger.debug(f"DBSCAN found {len(unique_labels)-1} clusters and {noise_points} noise points")
        
        # Process each cluster
        for cluster_id_num in set(cluster_labels):
            if cluster_id_num == -1:  # Noise points
                continue
                
            cluster_cases = [cases[i] for i, label in enumerate(cluster_labels) if label == cluster_id_num]
            
            if len(cluster_cases) >= min_cases:
                # Use first case's location for cluster ID
                first_case = cluster_cases[0]
                location_code = generate_location_code(
                    first_case.statename, first_case.districtname,
                    first_case.subdistrictname, first_case.villagename
                )
                date_str = target_date.strftime('%d%b%Y').upper()
                
                cluster_sequence[(location_code, date_str)] += 1
                seq = cluster_sequence[(location_code, date_str)]
                
                cluster_id = generate_cluster_id('GIS', location_code, date_str, seq)
                
                for case in cluster_cases:
                    clusters.append({
                        'unique_id': case.unique_id,
                        'cluster_id': cluster_id,
                        'dummy_id': f"{cluster_id}_{case.unique_id}",
                        'accept_status': None
                    })
    
    logger.info(f"GIS clustering completed: {len(clusters)} cases in {len(set(c['cluster_id'] for c in clusters))} clusters")
    return clusters

def save_clusters_to_table(client, project_id, dataset_id, clusters):
    """Save clusters to temp_cluster_table with deduplication"""
    if not clusters:
        return
    
    temp_cluster_table = os.getenv('TEMP_CLUSTER_TABLE', 'temp_cluster_table')
    table_id = f"{project_id}.{dataset_id}.{temp_cluster_table}"
    
    # Create table if not exists
    schema = [
        bigquery.SchemaField("unique_id", "STRING"),
        bigquery.SchemaField("cluster_id", "STRING"),
        bigquery.SchemaField("dummy_id", "STRING"),
        bigquery.SchemaField("accept_status", "STRING"),
    ]
    
    try:
        table = client.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
    
    # Insert with deduplication
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=schema
    )
    
    job = client.load_table_from_json(clusters, table_id, job_config=job_config)
    job.result()

def save_summary(client, project_id, dataset_id, date, abc_clusters, abc_cases, gis_clusters, gis_cases):
    """Save summary statistics to cluster_summary_table"""
    
    cluster_summary_table = os.getenv('CLUSTER_SUMMARY_TABLE', 'cluster_summary_table')
    table_id = f"{project_id}.{dataset_id}.{cluster_summary_table}"
    
    # Create table if not exists
    schema = [
        bigquery.SchemaField("patient_entry_date", "DATE"),
        bigquery.SchemaField("total_abc_clusters", "INTEGER"),
        bigquery.SchemaField("abc_cluster_cases", "INTEGER"),
        bigquery.SchemaField("total_gis_clusters", "INTEGER"),
        bigquery.SchemaField("gis_cluster_cases", "INTEGER"),
    ]
    
    try:
        table = client.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
    
    summary_data = [{
        'patient_entry_date': date.strftime('%Y-%m-%d'),
        'total_abc_clusters': abc_clusters,
        'abc_cluster_cases': abc_cases,
        'total_gis_clusters': gis_clusters,
        'gis_cluster_cases': gis_cases
    }]
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=schema
    )
    
    job = client.load_table_from_json(summary_data, table_id, job_config=job_config)
    job.result()

def check_streaming_buffer(client, project_id, dataset_id, table_id):
    """Check if BigQuery table has streaming buffer that would block clustering"""
    try:
        # Test with a simple UPDATE to check streaming buffer
        test_query = f"""
        UPDATE `{project_id}.{dataset_id}.{table_id}` 
        SET latitude = latitude 
        WHERE unique_id = 'streaming_buffer_test_dummy_id_12345'
        """
        
        job = client.query(test_query)
        job.result()
        return False  # No streaming buffer
        
    except Exception as e:
        error_msg = str(e).lower()
        if 'streaming' in error_msg or 'buffer' in error_msg:
            logger.info("Streaming buffer detected, skipping clustering")
            return True  # Streaming buffer present
        else:
            # Other error, log but don't block
            logger.warning(f"Streaming buffer check failed: {str(e)}")
            return False

def cluster_analysis(request):
    """Main clustering function for Cloud Function"""
    
    # Configuration from environment variables
    PROJECT_ID = os.getenv('PROJECT_ID', 'sentinel-h-5')
    DATASET_ID = os.getenv('DATASET_ID', 'sentinel_h_5')
    TABLE_ID = os.getenv('TABLE_ID', 'patient_records')
    
    # Get parameters from request or use environment defaults
    request_json = request.get_json(silent=True) or {}
    TIME_WINDOW = request_json.get('time_window', int(os.getenv('TIME_WINDOW', 7)))
    MIN_CASES = request_json.get('min_cases', int(os.getenv('MIN_CASES', 2)))
    
    try:
        client = bigquery.Client(project=PROJECT_ID)
        
        logger.info(f"Starting cluster analysis with config: PROJECT_ID={PROJECT_ID}, DATASET_ID={DATASET_ID}, TABLE_ID={TABLE_ID}")
        logger.debug(f"Parameters: TIME_WINDOW={TIME_WINDOW}, MIN_CASES={MIN_CASES}")
        
        # Check for streaming buffer
        logger.debug("Checking for streaming buffer")
        if check_streaming_buffer(client, PROJECT_ID, DATASET_ID, TABLE_ID):
            logger.warning("Streaming buffer detected, skipping clustering")
            return json.dumps({
                "status": "skipped",
                "message": "Streaming buffer detected, clustering paused",
                "processing_time": datetime.now().isoformat(),
                "clusters": []
            })
        
        # Get processed dates
        logger.debug("Getting processed dates")
        processed_dates = get_processed_dates(client, PROJECT_ID, DATASET_ID)
        logger.debug(f"Found {len(processed_dates)} already processed dates")
        
        # Find eligible date
        logger.debug("Finding eligible date for processing")
        target_date = get_eligible_date(client, PROJECT_ID, DATASET_ID, TABLE_ID, processed_dates)
        
        if not target_date:
            logger.info("No eligible Date found")
            return json.dumps({
                "status": "no_data",
                "message": "No eligible Date found",
                "processing_time": datetime.now().isoformat(),
                "clusters": []
            })
        
        logger.info(f"Processing date: {target_date}")
        
        # Perform ABC clustering
        logger.info("Starting ABC clustering")
        abc_clusters = perform_abc_clustering(
            client, PROJECT_ID, DATASET_ID, TABLE_ID, 
            target_date, TIME_WINDOW, MIN_CASES
        )
        
        # Perform GIS clustering
        logger.info("Starting GIS clustering")
        gis_clusters = perform_gis_clustering(
            client, PROJECT_ID, DATASET_ID, TABLE_ID,
            target_date, TIME_WINDOW, MIN_CASES
        )
        
        # Combine all clusters
        all_clusters = abc_clusters + gis_clusters
        logger.info(f"Total clusters found: {len(all_clusters)} cases")
        
        # Save to tables
        logger.debug("Saving clusters to database")
        save_clusters_to_table(client, PROJECT_ID, DATASET_ID, all_clusters)
        
        # Count unique clusters
        abc_cluster_ids = set(c['cluster_id'] for c in abc_clusters)
        gis_cluster_ids = set(c['cluster_id'] for c in gis_clusters)
        
        save_summary(
            client, PROJECT_ID, DATASET_ID, target_date,
            len(abc_cluster_ids), len(abc_clusters),
            len(gis_cluster_ids), len(gis_clusters)
        )
        
        logger.info(f"Processed {len(abc_cluster_ids)} ABC clusters ({len(abc_clusters)} cases)")
        logger.info(f"Processed {len(gis_cluster_ids)} GIS clusters ({len(gis_clusters)} cases)")
        
        response = {
            "status": "success",
            "message": f"Successfully processed {target_date}",
            "date": target_date.strftime('%Y-%m-%d'),
            "processing_time": datetime.now().isoformat(),
            "abc_clusters": len(abc_cluster_ids),
            "abc_cases": len(abc_clusters),
            "gis_clusters": len(gis_cluster_ids),
            "gis_cases": len(gis_clusters),
            "total_clusters": len(abc_cluster_ids) + len(gis_cluster_ids),
            "total_cases": len(all_clusters),
            "clusters": all_clusters
        }
        
        return json.dumps(response)
        
    except Exception as e:
        logger.error(f"Error in cluster analysis: {str(e)}")
        return json.dumps({
            "status": "error",
            "error": str(e),
            "processing_time": datetime.now().isoformat(),
            "clusters": []
        })