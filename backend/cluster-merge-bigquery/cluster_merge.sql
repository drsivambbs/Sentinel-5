-- Cluster Merge using BigQuery Scheduled Query
-- Run daily to merge duplicate clusters based on Jaccard similarity

DECLARE eligible_date DATE;
DECLARE start_date DATE;
DECLARE end_date DATE;

-- Find eligible date (last 4 days with <10% pending clusters)
SET eligible_date = (
  SELECT input_date
  FROM (
    SELECT input_date,
           COUNT(*) as total,
           COUNTIF(cluster_accepted_status = 'pending') as pending,
           SAFE_DIVIDE(COUNTIF(cluster_accepted_status = 'pending'), COUNT(*)) * 100 as pending_pct
    FROM `sentinel-h-5.sentinel_h_5.daily_detected_clusters`
    WHERE input_date >= DATE_SUB((SELECT MAX(input_date) FROM `sentinel-h-5.sentinel_h_5.daily_detected_clusters`), INTERVAL 4 DAY)
      AND input_date <= (SELECT MAX(input_date) FROM `sentinel-h-5.sentinel_h_5.daily_detected_clusters`)
    GROUP BY input_date
  )
  WHERE total > 0 AND pending_pct < 10
  ORDER BY input_date DESC
  LIMIT 1
);

-- Exit if no eligible date
IF eligible_date IS NULL THEN
  INSERT INTO `sentinel-h-5.sentinel_h_5.cluster_merge_log` VALUES (
    CONCAT('NO_ELIGIBLE_', FORMAT_TIMESTAMP('%Y%m%d_%H%M%S', CURRENT_TIMESTAMP())),
    CURRENT_TIMESTAMP(),
    NULL, NULL, 'NO_ELIGIBLE_DATE', 0, 0, 0, 0.0,
    'No eligible date found with <10% pending clusters'
  );
  RETURN;
END IF;

SET end_date = eligible_date;
SET start_date = DATE_SUB(end_date, INTERVAL 5 DAY);

-- Create temp table with cluster similarities
CREATE TEMP TABLE cluster_similarities AS
WITH cluster_patients AS (
  SELECT
    daily_cluster_id,
    algorithm_type,
    villagename,
    subdistrictname,
    ARRAY_AGG(DISTINCT CAST(unique_id AS STRING)) AS patient_ids
  FROM `sentinel-h-5.sentinel_h_5.daily_detected_clusters`
  WHERE input_date BETWEEN start_date AND end_date
    AND cluster_accepted_status = 'accepted'
  GROUP BY daily_cluster_id, algorithm_type, villagename, subdistrictname
),
abc_similarities AS (
  SELECT 
    c1.daily_cluster_id AS cluster1,
    c2.daily_cluster_id AS cluster2,
    SAFE_DIVIDE(
      (SELECT COUNT(*) FROM UNNEST(c1.patient_ids) AS id1 WHERE id1 IN UNNEST(c2.patient_ids)),
      ARRAY_LENGTH(ARRAY(SELECT DISTINCT id FROM UNNEST(ARRAY_CONCAT(c1.patient_ids, c2.patient_ids)) AS id))
    ) AS similarity
  FROM cluster_patients c1
  JOIN cluster_patients c2 ON c1.villagename = c2.villagename
  WHERE c1.daily_cluster_id < c2.daily_cluster_id
    AND c1.algorithm_type = 'ABC' AND c2.algorithm_type = 'ABC'
    AND c1.villagename IS NOT NULL
),
gis_similarities AS (
  SELECT 
    c1.daily_cluster_id AS cluster1,
    c2.daily_cluster_id AS cluster2,
    SAFE_DIVIDE(
      (SELECT COUNT(*) FROM UNNEST(c1.patient_ids) AS id1 WHERE id1 IN UNNEST(c2.patient_ids)),
      ARRAY_LENGTH(ARRAY(SELECT DISTINCT id FROM UNNEST(ARRAY_CONCAT(c1.patient_ids, c2.patient_ids)) AS id))
    ) AS similarity
  FROM cluster_patients c1
  JOIN cluster_patients c2 ON c1.subdistrictname = c2.subdistrictname
  WHERE c1.daily_cluster_id < c2.daily_cluster_id
    AND c1.algorithm_type = 'GIS' AND c2.algorithm_type = 'GIS'
    AND c1.subdistrictname IS NOT NULL
)
SELECT * FROM abc_similarities
UNION ALL
SELECT * FROM gis_similarities;

-- Process auto-merges (>50% similarity)
FOR record IN (SELECT cluster1, cluster2, similarity FROM cluster_similarities WHERE similarity > 0.50)
DO
  -- Delete overlapping records from cluster2
  DELETE FROM `sentinel-h-5.sentinel_h_5.daily_detected_clusters`
  WHERE daily_cluster_id = record.cluster2
    AND unique_id IN (
      SELECT unique_id 
      FROM `sentinel-h-5.sentinel_h_5.daily_detected_clusters` 
      WHERE daily_cluster_id = record.cluster1
    );
  
  -- Update remaining cluster2 records to cluster1
  UPDATE `sentinel-h-5.sentinel_h_5.daily_detected_clusters`
  SET daily_cluster_id = record.cluster1,
      merge_status = 'Auto_Merged',
      original_cluster_id = record.cluster2,
      jaccard_similarity = record.similarity
  WHERE daily_cluster_id = record.cluster2;
  
  -- Log the merge
  INSERT INTO `sentinel-h-5.sentinel_h_5.cluster_merge_log` VALUES (
    CONCAT(record.cluster1, '_', record.cluster2, '_', FORMAT_TIMESTAMP('%Y%m%d_%H%M%S', CURRENT_TIMESTAMP())),
    CURRENT_TIMESTAMP(),
    record.cluster1, record.cluster2, 'Auto_Merged',
    0, 0, 0, ROUND(record.similarity, 3), NULL
  );
END FOR;

-- Mark manual merges (20-59% similarity) - only best match per cluster
UPDATE `sentinel-h-5.sentinel_h_5.daily_detected_clusters`
SET merge_status = 'Manual_Merge_Pending',
    merge_candidate_id = s.cluster1,
    jaccard_similarity = s.similarity
FROM (
  SELECT cluster1, cluster2, similarity,
         ROW_NUMBER() OVER (PARTITION BY cluster2 ORDER BY similarity DESC) as rn
  FROM cluster_similarities
  WHERE similarity BETWEEN 0.20 AND 0.50
) s
WHERE daily_cluster_id = s.cluster2 AND s.rn = 1;

-- Mark no merges for remaining clusters
UPDATE `sentinel-h-5.sentinel_h_5.daily_detected_clusters`
SET merge_status = 'No_Merge_Found'
WHERE input_date BETWEEN start_date AND end_date
  AND merge_status IS NULL;