import json
import logging
import os
from datetime import datetime, timedelta
from math import radians, sin, cos, sqrt, atan2
from google.cloud import bigquery
from collections import defaultdict
import numpy as np
from sklearn.cluster import DBSCAN
from dotenv import load_dotenv
import functions_framework

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EARTH_RADIUS_METERS = 6371000.0

# ============================== UTILS ==============================
def haversine_distance(lat1, lon1, lat2, lon2):
    lat1r, lon1r, lat2r, lon2r = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2r - lat1r
    dlon = lon2r - lon1r
    a = sin(dlat/2)**2 + cos(lat1r) * cos(lat2r) * sin(dlon/2)**2
    return 2 * EARTH_RADIUS_METERS * sqrt(a) if a >= 0 else 0.0

def geodesic_centroid(lats, lons):
    lat_r, lon_r = np.radians(lats), np.radians(lons)
    x = np.cos(lat_r) * np.cos(lon_r)
    y = np.cos(lat_r) * np.sin(lon_r)
    z = np.sin(lat_r)
    x_m, y_m, z_m = np.mean(x), np.mean(y), np.mean(z)
    hyp = sqrt(x_m**2 + y_m**2)
    return np.degrees(atan2(z_m, hyp)), np.degrees(atan2(y_m, x_m))

def generate_location_code(statename, districtname, subdistrictname, villagename):
    parts = [statename, districtname, subdistrictname, villagename]
    code = "".join(p.strip()[0].upper() for p in parts if p and str(p).strip())
    return code or "UNK"

def generate_cluster_id(algo_type, location_code, syndrome, date_str, seq):
    clean_syn = "".join(c for c in str(syndrome) if c.isalnum()).upper()
    if len(clean_syn) >= 3:
        syn = clean_syn[0] + clean_syn[len(clean_syn)//2] + clean_syn[-1]
    elif len(clean_syn) == 2:
        syn = clean_syn + "X"
    elif len(clean_syn) == 1:
        syn = clean_syn + "XX"
    else:
        syn = "OTH"
    return f"{algo_type}_{location_code}_{syn}_{date_str}_{seq:03d}"

# ============================== CORE LOGIC ==============================
def get_recent_clusters(client, project_id, dataset_id, target_date, lookback_days=14):
    table_id = f"{project_id}.{dataset_id}.{os.getenv('TEMP_CLUSTER_TABLE', 'temp_cluster_table')}"
    try:
        client.get_table(table_id)
    except:
        return []  # Table doesn't exist yet, no recent clusters
    
    start_date = target_date - timedelta(days=lookback_days)
    sql = f"""
    SELECT DISTINCT cluster_id, cluster_centroid_lat AS lat, cluster_centroid_lon AS lon, algorithm_type
    FROM `{table_id}`
    WHERE analysis_input_date >= @start_date
      AND cluster_centroid_lat IS NOT NULL
      AND accept_status IN ('ACCEPTED', 'PENDING_MERGE')
      AND accept_status != 'REJECTED'
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "DATE", start_date)
    ])
    return list(client.query(sql, job_config=job_config))

def find_matching_cluster(lat, lon, recent_clusters, algo_type, max_dist=150):
    candidates = []
    for row in recent_clusters:
        if row.algorithm_type != algo_type: continue
        dist = haversine_distance(lat, lon, row.lat, row.lon)
        if dist <= max_dist:
            candidates.append((row.cluster_id, dist))
    
    if not candidates:
        return None, None, []
    
    # Sort by distance (closest first)
    candidates.sort(key=lambda x: x[1])
    closest_id, closest_dist = candidates[0]
    
    return closest_id, closest_dist, candidates[:3]  # Return top 3 candidates

def auto_accept_expired_clusters(client, project_id, dataset_id, target_date):
    table_id = f"{project_id}.{dataset_id}.{os.getenv('TEMP_CLUSTER_TABLE', 'temp_cluster_table')}"
    try:
        client.get_table(table_id)
    except:
        return  # Table doesn't exist yet
    
    timeout_days = int(os.getenv('AUTO_ACCEPT_TIMEOUT_DAYS', 3))
    cutoff_date = target_date - timedelta(days=timeout_days)
    
    sql = f"""
    UPDATE `{table_id}`
    SET accept_status = 'ACCEPTED'
    WHERE accept_status IN ('PENDING_MERGE', 'PENDING_NEW')
      AND analysis_input_date <= @cutoff_date
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("cutoff_date", "DATE", cutoff_date)
    ])
    job = client.query(sql, job_config=job_config)
    job.result()  # Wait for completion
    if job.num_dml_affected_rows and job.num_dml_affected_rows > 0:
        logger.info(f"Auto-accepted {job.num_dml_affected_rows} expired clusters")

def has_pending_clusters_blocking(client, project_id, dataset_id, target_date):
    table_id = f"{project_id}.{dataset_id}.{os.getenv('TEMP_CLUSTER_TABLE', 'temp_cluster_table')}"
    try:
        client.get_table(table_id)
    except:
        return False  # Table doesn't exist yet, no blocking clusters
    
    timeout_days = int(os.getenv('AUTO_ACCEPT_TIMEOUT_DAYS', 3))
    start_date = target_date - timedelta(days=30)
    
    sql = f"""
    SELECT 1
    FROM `{table_id}`
    WHERE analysis_input_date >= @start_date AND analysis_input_date < @target_date
      AND accept_status IN ('PENDING_MERGE', 'PENDING_NEW')
      AND analysis_input_date >= @target_date - @timeout_days
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
        bigquery.ScalarQueryParameter("target_date", "DATE", target_date),
        bigquery.ScalarQueryParameter("timeout_days", "INT64", timeout_days)
    ])
    return len(list(client.query(sql, job_config=job_config))) > 0

def get_eligible_date(client, project_id, dataset_id, table_id, processed_dates):
    limit_date = datetime.now().date() - timedelta(days=60)
    sql = f"""
    SELECT patient_entry_date,
           COUNT(*) total_cases,
           COUNT(IF(latitude IS NOT NULL, 1, 0)) * 100.0 / COUNT(*) AS geocoding_pct
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE patient_entry_date >= @limit_date
    GROUP BY patient_entry_date
    HAVING geocoding_pct >= @geocoding_threshold
    ORDER BY patient_entry_date ASC
    """
    geocoding_threshold = int(os.getenv('GEOCODING_THRESHOLD', 90))
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("limit_date", "DATE", limit_date),
        bigquery.ScalarQueryParameter("geocoding_threshold", "FLOAT64", geocoding_threshold)
    ])
    for row in client.query(sql, job_config=job_config):
        d = row.patient_entry_date.strftime('%Y-%m-%d')
        if d not in processed_dates:
            logger.info(f"Selected oldest eligible date: {d}")
            return row.patient_entry_date
    return None

def get_processed_dates(client, project_id, dataset_id):
    table = os.getenv('CLUSTER_SUMMARY_TABLE', 'cluster_summary_table')
    try:
        return {r.analysis_input_date.strftime('%Y-%m-%d') for r in client.query(f"SELECT DISTINCT analysis_input_date FROM `{project_id}.{dataset_id}.{table}`").result()}
    except Exception as e:
        logger.warning(f"Summary table not accessible: {e}")
        return set()

def check_streaming_buffer(client, project_id, dataset_id, table_id):
    try: 
        return bool(client.get_table(f"{project_id}.{dataset_id}.{table_id}").streaming_buffer)
    except: 
        return False

# ============================== SAVE FUNCTIONS ==============================
def save_clusters_to_table(client, project_id, dataset_id, clusters):
    if not clusters:
        logger.info("No clusters to save")
        return

    target_table = os.getenv('TEMP_CLUSTER_TABLE', 'temp_cluster_table')
    staging_table = 'staging_cluster_table'
    target_id = f"{project_id}.{dataset_id}.{target_table}"
    staging_id = f"{project_id}.{dataset_id}.{staging_table}"

    schema = [
        bigquery.SchemaField("unique_id", "STRING"),
        bigquery.SchemaField("cluster_id", "STRING"),
        bigquery.SchemaField("dummy_id", "STRING"),
        bigquery.SchemaField("accept_status", "STRING"),
        bigquery.SchemaField("statename", "STRING"),
        bigquery.SchemaField("districtname", "STRING"),
        bigquery.SchemaField("subdistrictname", "STRING"),
        bigquery.SchemaField("site_code", "STRING"),
        bigquery.SchemaField("villagename", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT64"),
        bigquery.SchemaField("longitude", "FLOAT64"),
        bigquery.SchemaField("patient_entry_date", "DATE"),
        bigquery.SchemaField("clini_primary_syn", "STRING"),
        bigquery.SchemaField("patient_name", "STRING"),
        bigquery.SchemaField("pat_age", "INTEGER"),
        bigquery.SchemaField("pat_sex", "INTEGER"),
        bigquery.SchemaField("full_address", "STRING"),
        bigquery.SchemaField("algorithm_type", "STRING"),
        bigquery.SchemaField("cluster_radius", "FLOAT64"),
        bigquery.SchemaField("cluster_case_count", "INTEGER"),
        bigquery.SchemaField("cluster_centroid_lat", "FLOAT64"),
        bigquery.SchemaField("cluster_centroid_lon", "FLOAT64"),
        bigquery.SchemaField("analysis_input_date", "DATE"),
        bigquery.SchemaField("matched_cluster_id", "STRING"),
        bigquery.SchemaField("match_distance_meters", "FLOAT64"),
        bigquery.SchemaField("match_confidence_score", "FLOAT64"),
        bigquery.SchemaField("candidate_clusters", "STRING"),  # JSON string of all candidates
    ]

    # Create optimized table with partitioning and clustering
    try:
        table = client.get_table(target_id)
        existing = {f.name for f in table.schema}
        new_fields = [f for f in schema if f.name not in existing]
        if new_fields:
            table.schema = table.schema + new_fields
            client.update_table(table, ['schema'])
    except:
        # Create table with partitioning and clustering for optimal performance
        table = bigquery.Table(target_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="analysis_input_date"
        )
        table.clustering_fields = ["accept_status", "algorithm_type", "statename", "districtname"]
        client.create_table(table)
        logger.info(f"Created optimized table {target_table} with partitioning and clustering")

    client.delete_table(staging_id, not_found_ok=True)
    client.create_table(bigquery.Table(staging_id, schema=schema))

    job = client.load_table_from_json(
        clusters, staging_id,
        job_config=bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
    )
    job.result()

    merge_sql = f"""
    MERGE `{target_id}` T USING `{staging_id}` S ON T.dummy_id = S.dummy_id
    WHEN MATCHED THEN UPDATE SET
      accept_status = S.accept_status,
      cluster_case_count = S.cluster_case_count,
      cluster_centroid_lat = S.cluster_centroid_lat,
      cluster_centroid_lon = S.cluster_centroid_lon,
      cluster_radius = S.cluster_radius
    WHEN NOT MATCHED THEN INSERT ROW
    """
    client.query(merge_sql).result()
    client.delete_table(staging_id, not_found_ok=True)
    logger.info(f"Saved {len(clusters)} cluster records")

def save_summary(client, project_id, dataset_id, patient_date, run_date, abc_accepted, abc_total, gis_accepted, gis_total):
    table_name = os.getenv('CLUSTER_SUMMARY_TABLE', 'cluster_summary_table')
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    schema = [
        bigquery.SchemaField("analysis_input_date", "DATE"),
        bigquery.SchemaField("cluster_analysis_run_date", "DATE"),
        bigquery.SchemaField("accepted_abc_clusters", "INTEGER"),
        bigquery.SchemaField("total_abc_cases", "INTEGER"),
        bigquery.SchemaField("accepted_gis_clusters", "INTEGER"),
        bigquery.SchemaField("total_gis_cases", "INTEGER"),
    ]
    try:
        client.get_table(table_id)
    except:
        # Create optimized summary table with partitioning
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="analysis_input_date"
        )
        table.clustering_fields = ["cluster_analysis_run_date"]
        client.create_table(table)
        logger.info(f"Created optimized summary table {table_name} with partitioning")

    data = [{
        "analysis_input_date": patient_date,
        "cluster_analysis_run_date": run_date,
        "accepted_abc_clusters": abc_accepted,
        "total_abc_cases": abc_total,
        "accepted_gis_clusters": gis_accepted,
        "total_gis_cases": gis_total
    }]
    client.load_table_from_json(data, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")).result()

# ============================== ABC CLUSTERING (RURAL) ==============================
def perform_abc_clustering(client, project_id, dataset_id, table_id, target_date, time_window, min_cases):
    start_date = target_date - timedelta(days=time_window)
    query = f"""
    SELECT unique_id, statename, districtname, subdistrictname, villagename, clini_primary_syn,
           patient_entry_date, latitude, longitude, site_code, patient_name, pat_age, pat_sex,
           CONCAT(COALESCE(pat_house,''),' ',COALESCE(pat_street,''),' ',COALESCE(villagename,''),' ',
                  COALESCE(subdistrictname,''),' ',COALESCE(districtname,''),' ',COALESCE(statename,''),' ',
                  COALESCE(SAFE_CAST(pat_pincode AS STRING),'')) AS full_address
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE pat_areatype = 'Rural' AND villagename IS NOT NULL
      AND latitude IS NOT NULL AND longitude IS NOT NULL
      AND patient_entry_date > @start_date AND patient_entry_date <= @target_date
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
        bigquery.ScalarQueryParameter("target_date", "DATE", target_date)
    ])
    rows = list(client.query(query, job_config=job_config))
    logger.info(f"ABC clustering: Found {len(rows)} rural cases for {target_date.strftime('%Y-%m-%d')}")

    clusters = []
    seq_counter = defaultdict(int)
    date_str = target_date.strftime('%d%b%Y').upper()
    recent_clusters = get_recent_clusters(client, project_id, dataset_id, target_date)

    groups = defaultdict(list)
    for r in rows:
        key = (r.statename, r.districtname, r.subdistrictname, r.villagename, r.clini_primary_syn)
        groups[key].append(r)

    for (statename, districtname, subdistrictname, villagename, syndrome), cases in groups.items():
        if len(cases) < min_cases: continue

        location_code = generate_location_code(statename, districtname, subdistrictname, villagename)
        lat_c, lon_c = geodesic_centroid([c.latitude for c in cases], [c.longitude for c in cases])

        match_id, dist, candidates = find_matching_cluster(lat_c, lon_c, recent_clusters, "ABC", 150)
        
        # Format candidates as JSON string
        candidates_json = json.dumps([{"cluster_id": cid, "distance_m": round(d, 1)} for cid, d in candidates]) if candidates else None

        if match_id and dist <= 50:
            cluster_id = match_id
            accept_status = "ACCEPTED"
            matched_cluster_id = match_id
            match_distance = dist
            confidence_score = 95.0 - (dist / 50.0 * 45.0)  # 95% at 0m, 50% at 50m
        elif match_id and dist <= 150:
            cluster_id = match_id
            accept_status = "PENDING_MERGE"
            matched_cluster_id = match_id
            match_distance = dist
            confidence_score = 50.0 - ((dist - 50.0) / 100.0 * 40.0)  # 50% at 50m, 10% at 150m
        else:
            seq_counter[(location_code, syndrome, date_str)] += 1
            seq = seq_counter[(location_code, syndrome, date_str)]
            cluster_id = generate_cluster_id('ABC', location_code, syndrome, date_str, seq)
            accept_status = "PENDING_NEW"
            matched_cluster_id = None
            match_distance = None
            confidence_score = 5.0  # Low confidence for new clusters
            candidates_json = None

        for c in cases:
            clusters.append({
                "unique_id": c.unique_id,
                "cluster_id": cluster_id,
                "dummy_id": f"{cluster_id}_{target_date.strftime('%Y%m%d')}_{c.unique_id}",
                "accept_status": accept_status,
                "statename": c.statename, "districtname": c.districtname, "subdistrictname": c.subdistrictname,
                "site_code": c.site_code, "villagename": c.villagename,
                "latitude": c.latitude, "longitude": c.longitude,
                "patient_entry_date": c.patient_entry_date.strftime('%Y-%m-%d'),
                "clini_primary_syn": c.clini_primary_syn,
                "patient_name": c.patient_name, "pat_age": c.pat_age, "pat_sex": c.pat_sex,
                "full_address": c.full_address,
                "algorithm_type": "ABC",
                "cluster_radius": None,
                "cluster_case_count": len(cases),
                "cluster_centroid_lat": round(lat_c, 6),
                "cluster_centroid_lon": round(lon_c, 6),
                "analysis_input_date": target_date.strftime('%Y-%m-%d'),
                "matched_cluster_id": matched_cluster_id,
                "match_distance_meters": round(match_distance, 2) if match_distance else None,
                "match_confidence_score": round(confidence_score, 1),
                "candidate_clusters": candidates_json
            })
    return clusters

# ============================== GIS CLUSTERING (URBAN) ==============================
def perform_gis_clustering(client, project_id, dataset_id, table_id, target_date, time_window, min_cases):
    start_date = target_date - timedelta(days=time_window)
    query = f"""
    SELECT unique_id, statename, districtname, subdistrictname, villagename, clini_primary_syn,
           patient_entry_date, latitude, longitude, site_code, patient_name, pat_age, pat_sex,
           CONCAT(COALESCE(pat_house,''),' ',COALESCE(pat_street,''),' ',COALESCE(villagename,''),' ',
                  COALESCE(subdistrictname,''),' ',COALESCE(districtname,''),' ',COALESCE(statename,''),' ',
                  COALESCE(SAFE_CAST(pat_pincode AS STRING),'')) AS full_address
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE pat_areatype = 'Urban'
      AND latitude IS NOT NULL AND longitude IS NOT NULL
      AND patient_entry_date > @start_date AND patient_entry_date <= @target_date
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
        bigquery.ScalarQueryParameter("target_date", "DATE", target_date)
    ])
    rows = list(client.query(query, job_config=job_config))
    logger.info(f"GIS clustering: Found {len(rows)} urban cases for {target_date.strftime('%Y-%m-%d')}")

    clusters = []
    seq_counter = defaultdict(int)
    date_str = target_date.strftime('%d%b%Y').upper()
    eps_meters = float(os.getenv('GIS_EPS_METERS', 350))
    eps_radians = eps_meters / EARTH_RADIUS_METERS
    recent_clusters = get_recent_clusters(client, project_id, dataset_id, target_date)

    syndrome_groups = defaultdict(list)
    for r in rows:
        syndrome_groups[r.clini_primary_syn].append(r)

    for syndrome, cases in syndrome_groups.items():
        if len(cases) < min_cases: continue
        cases = list(cases)

        coords_rad = np.radians([[c.latitude, c.longitude] for c in cases])
        db = DBSCAN(eps=eps_radians, min_samples=min_cases, metric='haversine').fit(coords_rad)
        labels = db.labels_

        for label in set(labels) - {-1}:
            idxs = np.where(labels == label)[0]
            if len(idxs) < min_cases: continue
            cluster_cases = [cases[i] for i in idxs]

            lat_c, lon_c = geodesic_centroid([c.latitude for c in cluster_cases], [c.longitude for c in cluster_cases])
            distances = [haversine_distance(lat_c, lon_c, c.latitude, c.longitude) for c in cluster_cases]
            radius_95p = float(np.percentile(distances, 95))
            rep_case = cluster_cases[np.argmin(distances)]
            location_code = generate_location_code(rep_case.statename, rep_case.districtname, rep_case.subdistrictname, rep_case.villagename)

            match_id, dist, candidates = find_matching_cluster(lat_c, lon_c, recent_clusters, "GIS", 150)
            
            # Format candidates as JSON string
            candidates_json = json.dumps([{"cluster_id": cid, "distance_m": round(d, 1)} for cid, d in candidates]) if candidates else None

            if match_id and dist <= 50:
                cluster_id = match_id
                accept_status = "ACCEPTED"
                matched_cluster_id = match_id
                match_distance = dist
                confidence_score = 95.0 - (dist / 50.0 * 45.0)  # 95% at 0m, 50% at 50m
            elif match_id and dist <= 150:
                cluster_id = match_id
                accept_status = "PENDING_MERGE"
                matched_cluster_id = match_id
                match_distance = dist
                confidence_score = 50.0 - ((dist - 50.0) / 100.0 * 40.0)  # 50% at 50m, 10% at 150m
            else:
                seq_counter[(location_code, syndrome, date_str)] += 1
                seq = seq_counter[(location_code, syndrome, date_str)]
                cluster_id = generate_cluster_id('GIS', location_code, syndrome, date_str, seq)
                accept_status = "PENDING_NEW"
                matched_cluster_id = None
                match_distance = None
                confidence_score = 5.0  # Low confidence for new clusters
                candidates_json = None

            for c in cluster_cases:
                clusters.append({
                    "unique_id": c.unique_id,
                    "cluster_id": cluster_id,
                    "dummy_id": f"{cluster_id}_{target_date.strftime('%Y%m%d')}_{c.unique_id}",
                    "accept_status": accept_status,
                    "statename": c.statename, "districtname": c.districtname, "subdistrictname": c.subdistrictname,
                    "site_code": c.site_code, "villagename": c.villagename,
                    "latitude": c.latitude, "longitude": c.longitude,
                    "patient_entry_date": c.patient_entry_date.strftime('%Y-%m-%d'),
                    "clini_primary_syn": c.clini_primary_syn,
                    "patient_name": c.patient_name, "pat_age": c.pat_age, "pat_sex": c.pat_sex,
                    "full_address": c.full_address,
                    "algorithm_type": "GIS",
                    "cluster_radius": round(radius_95p, 2),
                    "cluster_case_count": len(cluster_cases),
                    "cluster_centroid_lat": round(lat_c, 6),
                    "cluster_centroid_lon": round(lon_c, 6),
                    "analysis_input_date": target_date.strftime('%Y-%m-%d'),
                    "matched_cluster_id": matched_cluster_id,
                    "match_distance_meters": round(match_distance, 2) if match_distance else None,
                    "match_confidence_score": round(confidence_score, 1),
                    "candidate_clusters": candidates_json
                })
    return clusters

# ============================== MAIN ENTRY POINT ==============================
@functions_framework.http
def cluster_analysis(request):
    PROJECT_ID = os.getenv('PROJECT_ID', 'sentinel-h-5')
    DATASET_ID = os.getenv('DATASET_ID', 'sentinel_h_5')
    TABLE_ID = os.getenv('TABLE_ID', 'patient_records')
    TIME_WINDOW = int(os.getenv('TIME_WINDOW', 7))
    MIN_CASES = int(os.getenv('MIN_CASES', 2))

    client = bigquery.Client(project=PROJECT_ID)
    run_date = datetime.now().strftime('%Y-%m-%d')

    if check_streaming_buffer(client, PROJECT_ID, DATASET_ID, TABLE_ID):
        return json.dumps({"status": "skipped", "message": "Streaming buffer active"})

    processed = get_processed_dates(client, PROJECT_ID, DATASET_ID)
    target_date = get_eligible_date(client, PROJECT_ID, DATASET_ID, TABLE_ID, processed)

    if not target_date:
        return json.dumps({"status": "no_data", "message": "No new eligible date"})

    # Auto-accept expired clusters before checking for blocks
    auto_accept_expired_clusters(client, PROJECT_ID, DATASET_ID, target_date)
    
    if has_pending_clusters_blocking(client, PROJECT_ID, DATASET_ID, target_date):
        timeout_days = int(os.getenv('AUTO_ACCEPT_TIMEOUT_DAYS', 3))
        return json.dumps({
            "status": "blocked",
            "message": f"Pending human review (auto-accepts after {timeout_days} days)",
            "blocked_date": target_date.strftime('%Y-%m-%d')
        })

    abc = perform_abc_clustering(client, PROJECT_ID, DATASET_ID, TABLE_ID, target_date, TIME_WINDOW, MIN_CASES)
    gis = perform_gis_clustering(client, PROJECT_ID, DATASET_ID, TABLE_ID, target_date, TIME_WINDOW, MIN_CASES)
    all_clusters = abc + gis

    save_clusters_to_table(client, PROJECT_ID, DATASET_ID, all_clusters)
    save_summary(client, PROJECT_ID, DATASET_ID,
                 target_date.strftime('%Y-%m-%d'), run_date,
                 len({c['cluster_id'] for c in abc if c['accept_status'] == 'ACCEPTED'}),
                 len(abc),
                 len({c['cluster_id'] for c in gis if c['accept_status'] == 'ACCEPTED'}),
                 len(gis))

    # Calculate detailed statistics
    abc_accepted = len([c for c in abc if c['accept_status'] == 'ACCEPTED'])
    abc_pending_merge = len([c for c in abc if c['accept_status'] == 'PENDING_MERGE'])
    abc_pending_new = len([c for c in abc if c['accept_status'] == 'PENDING_NEW'])
    
    gis_accepted = len([c for c in gis if c['accept_status'] == 'ACCEPTED'])
    gis_pending_merge = len([c for c in gis if c['accept_status'] == 'PENDING_MERGE'])
    gis_pending_new = len([c for c in gis if c['accept_status'] == 'PENDING_NEW'])
    
    abc_unique_clusters = len({c['cluster_id'] for c in abc})
    gis_unique_clusters = len({c['cluster_id'] for c in gis})
    
    logger.info(f"Successfully processed {target_date.strftime('%Y-%m-%d')} | ABC: {len(abc)} cases, {abc_unique_clusters} clusters | GIS: {len(gis)} cases, {gis_unique_clusters} clusters")

    return json.dumps({
        "status": "success",
        "analysis_input_date": target_date.strftime('%Y-%m-%d'),
        "total_cases": len(all_clusters),
        "total_unique_clusters": abc_unique_clusters + gis_unique_clusters,
        "abc_clustering": {
            "total_cases": len(abc),
            "unique_clusters": abc_unique_clusters,
            "accepted_cases": abc_accepted,
            "pending_merge_cases": abc_pending_merge,
            "pending_new_cases": abc_pending_new
        },
        "gis_clustering": {
            "total_cases": len(gis),
            "unique_clusters": gis_unique_clusters,
            "accepted_cases": gis_accepted,
            "pending_merge_cases": gis_pending_merge,
            "pending_new_cases": gis_pending_new
        },
        "summary": {
            "total_accepted_cases": abc_accepted + gis_accepted,
            "total_pending_merge_cases": abc_pending_merge + gis_pending_merge,
            "total_pending_new_cases": abc_pending_new + gis_pending_new
        }
    })