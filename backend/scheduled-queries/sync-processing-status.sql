-- Scheduled query to sync processing status from transit table to main smart_clusters table
-- Run this every hour to keep processing_status updated

-- First, add the processing_status column if it doesn't exist
-- ALTER TABLE `sentinel-h-5.sentinel_h_5.smart_clusters` 
-- ADD COLUMN IF NOT EXISTS processing_status STRING;

-- Update processing_status based on smart_processing_status transit table
UPDATE `sentinel-h-5.sentinel_h_5.smart_clusters` sc
SET processing_status = CASE 
  WHEN sps.status = 'COMPLETED' THEN 'COMPLETED'
  ELSE 'PENDING'
END
FROM `sentinel-h-5.sentinel_h_5.smart_processing_status` sps
WHERE sc.original_creation_date = sps.date;

-- Set PENDING for dates not in processing status table
UPDATE `sentinel-h-5.sentinel_h_5.smart_clusters`
SET processing_status = 'PENDING'
WHERE processing_status IS NULL;