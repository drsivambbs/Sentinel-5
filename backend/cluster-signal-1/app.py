# ============================================================================
# DISEASE OUTBREAK CLUSTERING API - CLOUD RUN FLASK APPLICATION
# ============================================================================
# Deployment: Google Cloud Run with Flask
# Trigger: HTTP POST request or Cloud Scheduler
# ============================================================================

import os
import logging
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from google.cloud import bigquery

# Initialize Flask app
app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
PROJECT_ID = os.environ.get('PROJECT_ID', 'sentinel-h-5')
DATASET_ID = os.environ.get('DATASET_ID', 'sentinel_h_5')
SOURCE_TABLE_ID = os.environ.get('SOURCE_TABLE_ID', 'patient_records')
TARGET_TABLE_ID = os.environ.get('TARGET_TABLE_ID', 'daily_cluster')

# Full table paths
SOURCE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE_ID}"
TARGET_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TARGET_TABLE_ID}"

# Column Names
PATIENT_ENTRY_DATE = 'patient_entry_date'
UNIQUE_ID = 'unique_id'
CLINICAL_PRIMARY_SYNDROME = 'clini_primary_syn'
VILLAGE_NAME = 'villagename'
AREA_TYPE = 'pat_areatype'
LATITUDE = 'latitude'
LONGITUDE = 'longitude'
STATE = 'statename'
DISTRICT = 'districtname'
SUBDISTRICT = 'subdistrictname'

# Clustering Parameters
GIS_DBSCAN_EPSILON = int(os.environ.get('GIS_DBSCAN_EPSILON', 400))
GIS_MAX_CLUSTER_RADIUS = int(os.environ.get('GIS_MAX_CLUSTER_RADIUS', 750))
MIN_CLUSTER_SIZE = int(os.environ.get('MIN_CLUSTER_SIZE', 2))
LOOKBACK_DAYS = int(os.environ.get('LOOKBACK_DAYS', 7))

# Initialize BigQuery Client
client = bigquery.Client(project=PROJECT_ID)

# --- TABLE SETUP ---
def create_daily_cluster_table():
    """Creates the daily_cluster table if it doesn't exist"""
    try:
        source_ref = client.dataset(DATASET_ID).table(SOURCE_TABLE_ID)
        source_table = client.get_table(source_ref)
        
        target_schema = list(source_table.schema)
        
        clustering_fields = [
            bigquery.SchemaField("algorithm_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("cluster_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("input_date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("dummy_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("actual_cluster_radius", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("accept_status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("merge_status", "STRING", mode="NULLABLE"),      # ADD THIS
            bigquery.SchemaField("cluster_duration", "INT64", mode="NULLABLE"),   # ADD THIS
        ]
        
        target_schema.extend(clustering_fields)
        
        target_ref = client.dataset(DATASET_ID).table(TARGET_TABLE_ID)
        target_table = bigquery.Table(target_ref, schema=target_schema)
        
        client.create_table(target_table, exists_ok=True)
        logger.info(f"Table ensured: {TARGET_TABLE}")
        return True
        
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        return False


# --- DATE FINDING ---
def find_next_processing_date():
    """Find the next unprocessed date"""
    query = f"""
    WITH max_source_date AS (
        SELECT MAX({PATIENT_ENTRY_DATE}) AS max_date 
        FROM `{SOURCE_TABLE}`
    ),
    date_range AS (
        SELECT 
            DATE_SUB(max_date, INTERVAL offset_days DAY) AS check_date
        FROM max_source_date
        CROSS JOIN UNNEST(GENERATE_ARRAY(0, 10)) AS offset_days
    ),
    processed_dates AS (
        SELECT DISTINCT input_date 
        FROM `{TARGET_TABLE}`
    )
    SELECT check_date
    FROM date_range
    LEFT JOIN processed_dates ON date_range.check_date = processed_dates.input_date
    WHERE processed_dates.input_date IS NULL
    ORDER BY check_date DESC
    LIMIT 1
    """
    
    try:
        query_job = client.query(query)
        result = query_job.result()
        
        row = next(result, None)
        if row:
            processing_date = row.check_date
            frame_end = processing_date
            frame_start = frame_end - timedelta(days=LOOKBACK_DAYS - 1)
            
            return (
                processing_date.strftime('%Y-%m-%d'),
                frame_start.strftime('%Y-%m-%d'),
                frame_end.strftime('%Y-%m-%d')
            )
        else:
            return None, None, None
            
    except Exception as e:
        logger.error(f"Error finding date: {e}")
        return None, None, None


# --- CLUSTERING QUERIES ---
def build_rural_abc_query(processing_date: str, frame_start: str, frame_end: str):
    """ABC Algorithm for Rural Areas"""
    return f"""
    INSERT INTO `{TARGET_TABLE}`
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
            ) AS cluster_count,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    DATE('{processing_date}'),
                    t.{STATE},
                    t.{CLINICAL_PRIMARY_SYNDROME}
                ORDER BY t.{VILLAGE_NAME}
            ) AS cluster_sequence_num
        FROM `{SOURCE_TABLE}` AS t
        WHERE 
            t.{PATIENT_ENTRY_DATE} BETWEEN DATE('{frame_start}') AND DATE('{frame_end}')
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
            t1.*,
            CONCAT(
                'ABC-',
                CAST(FARM_FINGERPRINT(CONCAT(
                    t1.{VILLAGE_NAME}, 
                    t1.{CLINICAL_PRIMARY_SYNDROME}
                )) AS STRING),
                '-',
                FORMAT_DATE('%d%m%y', DATE('{processing_date}')),
                '-',
                LPAD(CAST(t1.cluster_sequence_num AS STRING), 3, '0')
            ) AS cluster_id,
            'ABC' AS algorithm_type,
            DATE('{processing_date}') AS input_date,
            0.0 AS actual_cluster_radius,
            'Pending' AS accept_status
        FROM filtered_clusters AS t1
    )
    SELECT
        t2.* EXCEPT(
            cluster_count, 
            cluster_sequence_num,
            cluster_id, 
            algorithm_type, 
            input_date, 
            actual_cluster_radius, 
            accept_status
        ),
        t2.algorithm_type,
        t2.cluster_id,
        t2.input_date,
        CONCAT(t2.cluster_id, '-', t2.{UNIQUE_ID}) AS dummy_id,
        t2.actual_cluster_radius,
        t2.accept_status,
        CAST(NULL AS STRING) AS merge_status,
        CAST(NULL AS INT64) AS cluster_duration
    FROM clustered_data AS t2
    """


def build_urban_gis_query(processing_date: str, frame_start: str, frame_end: str):
    """GIS Algorithm for Urban Areas"""
    return f"""
    INSERT INTO `{TARGET_TABLE}`
    WITH labeled_data AS (
        SELECT
            t.*,
            ST_CLUSTERDBSCAN(
                ST_GEOGPOINT(t.{LONGITUDE}, t.{LATITUDE}), 
                {GIS_DBSCAN_EPSILON}, 
                {MIN_CLUSTER_SIZE}
            ) OVER (PARTITION BY t.{CLINICAL_PRIMARY_SYNDROME}) AS cluster_label,
            ST_GEOGPOINT(t.{LONGITUDE}, t.{LATITUDE}) AS patient_point
        FROM `{SOURCE_TABLE}` AS t
        WHERE 
            t.{PATIENT_ENTRY_DATE} BETWEEN DATE('{frame_start}') AND DATE('{frame_end}')
            AND t.{AREA_TYPE} = 'Urban'
            AND t.{LATITUDE} IS NOT NULL
            AND t.{LONGITUDE} IS NOT NULL
            AND t.{LATITUDE} != 0.0
            AND t.{LONGITUDE} != 0.0
    ),
    centroid_calculation AS (
        SELECT 
            {CLINICAL_PRIMARY_SYNDROME},
            cluster_label,
            ST_CENTROID(ST_UNION_AGG(patient_point)) AS cluster_centroid
        FROM labeled_data
        WHERE cluster_label != 0
        GROUP BY 1, 2
    ),
    radius_calculation AS (
        SELECT
            t1.* EXCEPT(patient_point),
            MAX(ST_DISTANCE(t1.patient_point, t2.cluster_centroid)) 
                OVER (
                    PARTITION BY 
                        t1.{CLINICAL_PRIMARY_SYNDROME}, 
                        t1.cluster_label
                ) AS actual_cluster_radius,
            COUNT(*) OVER (
                PARTITION BY 
                    t1.{CLINICAL_PRIMARY_SYNDROME}, 
                    t1.cluster_label
            ) AS cluster_size
        FROM labeled_data AS t1
        INNER JOIN centroid_calculation AS t2
            ON t1.{CLINICAL_PRIMARY_SYNDROME} = t2.{CLINICAL_PRIMARY_SYNDROME}
            AND t1.cluster_label = t2.cluster_label
        WHERE t1.cluster_label != 0
    ),
    valid_clusters AS (
        SELECT * EXCEPT(cluster_size)
        FROM radius_calculation
        WHERE cluster_size >= {MIN_CLUSTER_SIZE}
          AND actual_cluster_radius <= {GIS_MAX_CLUSTER_RADIUS}
    ),
    clustered_data AS (
        SELECT
            t3.*,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    DATE('{processing_date}'), 
                    t3.{CLINICAL_PRIMARY_SYNDROME}
                ORDER BY t3.cluster_label
            ) AS cluster_sequence_num,
            'GIS' AS algorithm_type,
            DATE('{processing_date}') AS input_date,
            'Pending' AS accept_status
        FROM valid_clusters AS t3
    ),
    final_metadata AS (
        SELECT
            t4.*,
            CONCAT(
                'GIS-',
                CAST(t4.cluster_label AS STRING),
                '-',
                FORMAT_DATE('%d%m%y', DATE('{processing_date}')),
                '-',
                LPAD(CAST(t4.cluster_sequence_num AS STRING), 3, '0')
            ) AS cluster_id
        FROM clustered_data AS t4
    )
    SELECT
        t5.* EXCEPT(
            cluster_label,
            cluster_sequence_num,
            algorithm_type,
            input_date,
            accept_status,
            actual_cluster_radius,
            cluster_id
        ),
        t5.algorithm_type,
        t5.cluster_id,
        t5.input_date,
        CONCAT(t5.cluster_id, '-', t5.{UNIQUE_ID}) AS dummy_id,
        t5.actual_cluster_radius,
        t5.accept_status,
        CAST(NULL AS STRING) AS merge_status,
        CAST(NULL AS INT64) AS cluster_duration
    FROM final_metadata AS t5
    """


def run_clustering_job(processing_date: str, frame_start: str, frame_end: str):
    """Execute both clustering algorithms"""
    total_rows = 0
    results = {
        'abc_rows': 0,
        'gis_rows': 0,
        'abc_success': False,
        'gis_success': False,
        'abc_error': None,
        'gis_error': None
    }
    
    # Rural ABC
    try:
        logger.info(f"Running RURAL (ABC) clustering for {processing_date}")
        abc_query = build_rural_abc_query(processing_date, frame_start, frame_end)
        abc_job = client.query(abc_query)
        abc_job.result()
        
        abc_rows = abc_job.num_dml_affected_rows
        total_rows += abc_rows
        results['abc_rows'] = abc_rows
        results['abc_success'] = True
        logger.info(f"RURAL (ABC) complete: {abc_rows} rows")
        
    except Exception as e:
        logger.error(f"RURAL (ABC) failed: {e}")
        results['abc_error'] = str(e)
    
    # Urban GIS
    try:
        logger.info(f"Running URBAN (GIS) clustering for {processing_date}")
        gis_query = build_urban_gis_query(processing_date, frame_start, frame_end)
        gis_job = client.query(gis_query)
        gis_job.result()
        
        gis_rows = gis_job.num_dml_affected_rows
        total_rows += gis_rows
        results['gis_rows'] = gis_rows
        results['gis_success'] = True
        logger.info(f"URBAN (GIS) complete: {gis_rows} rows")
        
    except Exception as e:
        logger.error(f"URBAN (GIS) failed: {e}")
        results['gis_error'] = str(e)
    
    results['total_rows'] = total_rows
    return results


# --- FLASK ROUTES ---

@app.route('/', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'disease-clustering-api',
        'timestamp': datetime.utcnow().isoformat()
    }), 200


@app.route('/run-clustering', methods=['POST'])
def run_clustering():
    """Main endpoint to trigger clustering job"""
    try:
        logger.info("=" * 70)
        logger.info("Clustering job triggered via API")
        
        # Setup tables
        if not create_daily_cluster_table():
            return jsonify({
                'success': False,
                'error': 'Failed to setup tables'
            }), 500
        
        # Find next date
        processing_date, frame_start, frame_end = find_next_processing_date()
        
        if not processing_date:
            return jsonify({
                'success': True,
                'message': 'No new data to process',
                'processing_date': None,
                'total_rows': 0
            }), 200
        
        # Run clustering
        logger.info(f"Processing date: {processing_date}")
        logger.info(f"Analysis window: {frame_start} to {frame_end}")
        
        results = run_clustering_job(processing_date, frame_start, frame_end)
        
        response = {
            'success': True,
            'processing_date': processing_date,
            'frame_start': frame_start,
            'frame_end': frame_end,
            'total_rows': results['total_rows'],
            'abc_clusters': results['abc_rows'],
            'gis_clusters': results['gis_rows'],
            'abc_success': results['abc_success'],
            'gis_success': results['gis_success'],
            'errors': {
                'abc': results['abc_error'],
                'gis': results['gis_error']
            },
            'parameters': {
                'gis_epsilon': GIS_DBSCAN_EPSILON,
                'gis_max_radius': GIS_MAX_CLUSTER_RADIUS,
                'min_cluster_size': MIN_CLUSTER_SIZE,
                'lookback_days': LOOKBACK_DAYS
            },
            'timestamp': datetime.utcnow().isoformat()
        }
        
        logger.info(f"Job complete: {results['total_rows']} rows inserted")
        logger.info("=" * 70)
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"Clustering job failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500


@app.route('/status', methods=['GET'])
def get_status():
    """Get current pipeline status"""
    try:
        # Get latest processed date
        query = f"""
        SELECT 
            MAX(input_date) as last_processed_date,
            COUNT(DISTINCT cluster_id) as total_clusters,
            COUNT(*) as total_records
        FROM `{TARGET_TABLE}`
        """
        
        result = client.query(query).result()
        row = next(result, None)
        
        return jsonify({
            'success': True,
            'last_processed_date': row.last_processed_date.isoformat() if row.last_processed_date else None,
            'total_clusters': row.total_clusters if row else 0,
            'total_records': row.total_records if row else 0,
            'parameters': {
                'gis_epsilon': GIS_DBSCAN_EPSILON,
                'gis_max_radius': GIS_MAX_CLUSTER_RADIUS,
                'min_cluster_size': MIN_CLUSTER_SIZE,
                'lookback_days': LOOKBACK_DAYS
            },
            'timestamp': datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# --- MAIN ---
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)