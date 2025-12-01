-- BigQuery Table Partitioning Optimization for Smart Clustering Engine
-- This script creates partitioned versions of tables to improve query performance

-- 1. Create partitioned patient_records table
CREATE OR REPLACE TABLE `sentinel-h-5.sentinel_h_5.patient_records_partitioned`
PARTITION BY patient_entry_date
CLUSTER BY pat_areatype, clini_primary_syn, statename, districtname
AS SELECT * FROM `sentinel-h-5.sentinel_h_5.patient_records`;

-- 2. Create partitioned smart_clusters table
CREATE OR REPLACE TABLE `sentinel-h-5.sentinel_h_5.smart_clusters_partitioned`
PARTITION BY original_creation_date
CLUSTER BY algorithm_type, accept_status, primary_syndrome
AS SELECT * FROM `sentinel-h-5.sentinel_h_5.smart_clusters`;

-- 3. Create partitioned smart_assignments table
CREATE OR REPLACE TABLE `sentinel-h-5.sentinel_h_5.smart_cluster_assignments_partitioned`
PARTITION BY DATE(assigned_at)
CLUSTER BY smart_cluster_id, addition_type
AS SELECT * FROM `sentinel-h-5.sentinel_h_5.smart_cluster_assignments`;

-- 4. Create partitioned processing status table
CREATE OR REPLACE TABLE `sentinel-h-5.sentinel_h_5.smart_processing_status_partitioned`
PARTITION BY date
CLUSTER BY status, worker_id
AS SELECT * FROM `sentinel-h-5.sentinel_h_5.smart_processing_status`;

-- Performance Analysis Queries
-- Check partition pruning effectiveness
SELECT 
  table_name,
  partition_id,
  total_rows,
  total_logical_bytes,
  total_billable_bytes
FROM `sentinel-h-5.sentinel_h_5.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name IN ('patient_records_partitioned', 'smart_clusters_partitioned')
ORDER BY table_name, partition_id;

-- Test query performance comparison
-- Original query (full scan)
SELECT COUNT(*) as original_scan
FROM `sentinel-h-5.sentinel_h_5.patient_records`
WHERE patient_entry_date BETWEEN '2024-01-01' AND '2024-01-07'
  AND pat_areatype = 'Urban';

-- Partitioned query (partition pruning)
SELECT COUNT(*) as partitioned_scan
FROM `sentinel-h-5.sentinel_h_5.patient_records_partitioned`
WHERE patient_entry_date BETWEEN '2024-01-01' AND '2024-01-07'
  AND pat_areatype = 'Urban';