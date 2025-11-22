-- Manual approval of pending cluster merges
-- Replace @cluster_id with actual cluster ID to approve

DECLARE target_cluster_id STRING DEFAULT @cluster_id;
DECLARE candidate_id STRING;
DECLARE similarity_score FLOAT64;

-- Get merge candidate info
SET (candidate_id, similarity_score) = (
  SELECT AS STRUCT merge_candidate_id, jaccard_similarity
  FROM `sentinel-h-5.sentinel_h_5.daily_detected_clusters`
  WHERE daily_cluster_id = target_cluster_id
    AND merge_status = 'Manual_Merge_Pending'
  LIMIT 1
);

-- Exit if no pending merge found
IF candidate_id IS NULL THEN
  SELECT 'No pending merge found for cluster: ' || target_cluster_id as error;
  RETURN;
END IF;

-- Delete overlapping records from target cluster
DELETE FROM `sentinel-h-5.sentinel_h_5.daily_detected_clusters`
WHERE daily_cluster_id = target_cluster_id
  AND unique_id IN (
    SELECT unique_id 
    FROM `sentinel-h-5.sentinel_h_5.daily_detected_clusters` 
    WHERE daily_cluster_id = candidate_id
  );

-- Update remaining records to candidate cluster
UPDATE `sentinel-h-5.sentinel_h_5.daily_detected_clusters`
SET daily_cluster_id = candidate_id,
    merge_status = 'Manual_Merged',
    original_cluster_id = target_cluster_id,
    jaccard_similarity = similarity_score,
    reviewed_by = 'admin',
    review_timestamp = CURRENT_TIMESTAMP()
WHERE daily_cluster_id = target_cluster_id;

-- Log the approval
INSERT INTO `sentinel-h-5.sentinel_h_5.cluster_merge_log` VALUES (
  CONCAT(candidate_id, '_', target_cluster_id, '_', FORMAT_TIMESTAMP('%Y%m%d_%H%M%S', CURRENT_TIMESTAMP())),
  CURRENT_TIMESTAMP(),
  candidate_id, target_cluster_id, 'Manual_Merged',
  0, 0, 0, similarity_score, NULL
);

SELECT 'Cluster merge approved successfully' as result;