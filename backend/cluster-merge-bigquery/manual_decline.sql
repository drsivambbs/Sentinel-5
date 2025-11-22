-- Manual decline of pending cluster merges
-- Replace @cluster_id with actual cluster ID to decline

DECLARE target_cluster_id STRING DEFAULT @cluster_id;

-- Update cluster status to declined
UPDATE `sentinel-h-5.sentinel_h_5.daily_detected_clusters`
SET merge_status = 'Manual_Merge_Declined',
    reviewed_by = 'admin',
    review_timestamp = CURRENT_TIMESTAMP()
WHERE daily_cluster_id = target_cluster_id
  AND merge_status = 'Manual_Merge_Pending';

-- Check if update was successful
IF @@row_count = 0 THEN
  SELECT 'No pending merge found for cluster: ' || target_cluster_id as error;
ELSE
  SELECT 'Cluster merge declined successfully' as result;
END IF;