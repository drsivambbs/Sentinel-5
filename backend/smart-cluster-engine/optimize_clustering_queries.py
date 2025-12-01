#!/usr/bin/env python3
"""
Optimized clustering queries using partitioned tables
Replace existing queries in app.py with these optimized versions
"""

# Optimized ABC clustering query with partition pruning
OPTIMIZED_ABC_QUERY = """
WITH base_clusters AS (
    SELECT 
        t.*,
        COUNT(*) OVER (
            PARTITION BY 
                t.statename,
                t.districtname,
                t.subdistrictname,
                t.villagename,
                t.clini_primary_syn
        ) AS cluster_count
    FROM `sentinel-h-5.sentinel_h_5.patient_records_partitioned` AS t
    WHERE 
        t.patient_entry_date BETWEEN DATE_SUB(@processing_date, INTERVAL 7 DAY) 
                                 AND DATE_SUB(@processing_date, INTERVAL 1 DAY)
        AND t.pat_areatype = 'Rural'
        AND t.villagename IS NOT NULL
)
SELECT *
FROM base_clusters
WHERE cluster_count >= @min_cluster_size
"""

# Optimized GIS clustering query with partition pruning
OPTIMIZED_GIS_QUERY = """
SELECT 
    unique_id,
    clini_primary_syn,
    latitude,
    longitude,
    patient_entry_date
FROM `sentinel-h-5.sentinel_h_5.patient_records_partitioned`
WHERE 
    patient_entry_date BETWEEN DATE_SUB(@processing_date, INTERVAL 7 DAY) 
                           AND DATE_SUB(@processing_date, INTERVAL 1 DAY)
    AND pat_areatype = 'Urban'
    AND latitude BETWEEN 8 AND 37
    AND longitude BETWEEN 68 AND 97
    AND latitude != 0.0
    AND longitude != 0.0
    AND (pat_street IS NOT NULL AND pat_street != '' 
         OR villagename IS NOT NULL AND villagename != '')
"""

# Optimized cluster lookup queries
OPTIMIZED_ABC_CLUSTER_LOOKUP = """
SELECT 
    c.smart_cluster_id,
    c.original_creation_date,
    c.patient_count,
    c.expansion_count
FROM `sentinel-h-5.sentinel_h_5.smart_clusters_partitioned` c
WHERE c.algorithm_type = 'ABC'
  AND c.village_name = @village
  AND c.primary_syndrome = @syndrome
  AND c.original_creation_date >= DATE_SUB(@processing_date, INTERVAL @max_age_days DAY)
ORDER BY c.original_creation_date ASC
LIMIT 1
"""

OPTIMIZED_GIS_CLUSTER_LOOKUP = """
SELECT 
    c.smart_cluster_id,
    c.original_creation_date,
    c.patient_count,
    c.expansion_count,
    c.centroid_lat,
    c.centroid_lon,
    c.actual_cluster_radius
FROM `sentinel-h-5.sentinel_h_5.smart_clusters_partitioned` c
WHERE c.algorithm_type = 'GIS'
  AND c.primary_syndrome = @syndrome
  AND c.original_creation_date >= DATE_SUB(@processing_date, INTERVAL @max_age_days DAY)
  AND ST_DISTANCE(
      ST_GEOGPOINT(c.centroid_lon, c.centroid_lat),
      ST_GEOGPOINT(@lon, @lat)
  ) <= @epsilon_m
ORDER BY c.original_creation_date ASC
LIMIT 1
"""

# Optimized assignment lookup with partition pruning
OPTIMIZED_ASSIGNMENT_LOOKUP = """
SELECT p.unique_id, p.latitude, p.longitude, p.patient_entry_date
FROM `sentinel-h-5.sentinel_h_5.smart_cluster_assignments_partitioned` a
JOIN `sentinel-h-5.sentinel_h_5.patient_records_partitioned` p ON a.unique_id = p.unique_id
WHERE a.smart_cluster_id = @cluster_id
  AND p.latitude IS NOT NULL AND p.longitude IS NOT NULL
  AND DATE(a.assigned_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
"""

# Performance monitoring query
PERFORMANCE_MONITORING_QUERY = """
SELECT 
    job_id,
    creation_time,
    start_time,
    end_time,
    TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) as duration_ms,
    total_bytes_processed,
    total_slot_ms,
    query
FROM `sentinel-h-5.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND job_type = 'QUERY'
  AND query LIKE '%smart_clusters%'
ORDER BY creation_time DESC
LIMIT 10
"""

def get_optimization_recommendations():
    """Return optimization recommendations for clustering performance"""
    return {
        "partitioning": {
            "patient_records": "Partition by patient_entry_date, cluster by pat_areatype, clini_primary_syn",
            "smart_clusters": "Partition by original_creation_date, cluster by algorithm_type, accept_status",
            "smart_assignments": "Partition by DATE(assigned_at), cluster by smart_cluster_id"
        },
        "query_optimizations": [
            "Use partition pruning with date filters",
            "Add clustering columns to WHERE clauses",
            "Limit lookback periods to reduce scan size",
            "Use parameterized queries to enable query caching",
            "Consider materialized views for frequently accessed data"
        ],
        "performance_monitoring": [
            "Monitor query execution times",
            "Track bytes processed per query",
            "Set up alerts for slow queries (>30 seconds)",
            "Use query labels for better tracking"
        ]
    }

if __name__ == "__main__":
    recommendations = get_optimization_recommendations()
    print("BigQuery Optimization Recommendations:")
    for category, items in recommendations.items():
        print(f"\n{category.upper()}:")
        if isinstance(items, dict):
            for table, recommendation in items.items():
                print(f"  {table}: {recommendation}")
        else:
            for item in items:
                print(f"  - {item}")