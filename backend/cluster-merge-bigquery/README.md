# BigQuery Cluster Merge

Pure BigQuery approach for cluster merging using scheduled queries.

## Features
- **Bulletproof**: Atomic BigQuery operations
- **Simple**: No external services or custom code
- **Built-in**: Uses BigQuery's native ML.JACCARD_INDEX
- **Reliable**: Automatic retries and error handling

## Setup

1. **Deploy scheduled query**:
```bash
cd backend/cluster-merge-bigquery
chmod +x setup.sh
./setup.sh
```

2. **Verify deployment**:
- Go to BigQuery Console → Scheduled Queries
- Find "Daily Cluster Merge" 
- Runs daily at 2 AM Asia/Kolkata

## Manual Operations

**View pending merges**:
```sql
SELECT daily_cluster_id, merge_candidate_id, jaccard_similarity, 
       algorithm_type, villagename, subdistrictname
FROM `sentinel-h-5.sentinel_h_5.daily_detected_clusters`
WHERE merge_status = 'Manual_Merge_Pending'
ORDER BY jaccard_similarity DESC;
```

**Approve a merge**:
```sql
DECLARE cluster_id STRING DEFAULT 'your_cluster_id_here';
-- Then run manual_approve.sql
```

**Decline a merge**:
```sql
DECLARE cluster_id STRING DEFAULT 'your_cluster_id_here';  
-- Then run manual_decline.sql
```

**Check merge status**:
```sql
SELECT merge_status, COUNT(*) as count
FROM `sentinel-h-5.sentinel_h_5.daily_detected_clusters`
GROUP BY merge_status;
```

## Advantages over Cloud Run

- ✅ **No race conditions** - BigQuery handles concurrency
- ✅ **Atomic operations** - All-or-nothing transactions  
- ✅ **Built-in retries** - BigQuery handles failures
- ✅ **No deployment** - Just SQL scripts
- ✅ **Cost effective** - Only pay for query execution
- ✅ **Monitoring** - Built into BigQuery console
- ✅ **Scaling** - BigQuery handles any data size

## Monitoring

- **BigQuery Console** → Scheduled Queries → Daily Cluster Merge
- **Job History** shows execution status and errors
- **Query Results** show merge statistics
- **Logs** in `sentinel_h_5.cluster_merge_log` table