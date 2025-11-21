# Clustering Function

Automated patient data clustering system using Area-Based Clustering (ABC) and GIS-Based Clustering with DBSCAN.

## Features

- **Area-Based Clustering**: Rural areas grouped by village and syndrome
- **GIS-Based Clustering**: Urban areas using DBSCAN with 500m radius
- **Automated Processing**: 30-minute Cloud Scheduler intervals
- **Deduplication**: Prevents duplicate cluster entries
- **Progress Tracking**: Summary statistics and processing history

## Deployment

```bash
# Deploy function
./deploy.sh

# Setup automation scheduler
./scheduler_setup.sh
```

## Configuration

Default parameters (configurable via request):
- `time_window`: 7 days
- `min_cases`: 2 cases per cluster
- `geocoding_threshold`: 90%
- `date_range_limit`: 15 days from max date

## Testing

```bash
# Test deployed function
python test_function.py

# Local testing
functions-framework --target=cluster_analysis --debug
```

## Output Tables

### temp_cluster_table
- `unique_id`: Patient unique identifier
- `cluster_id`: Generated cluster ID (ABC_GAKA_15NOV2025_001)
- `dummy_id`: Deduplication key (cluster_id + unique_id)
- `accept_status`: Manual review status (empty)

### cluster_summary_table
- `Date`: Processing date
- `Total_ABC_Clusters`: Count of ABC clusters
- `ABC_Cluster_Cases`: Count of ABC cases
- `Total_GIS_Clusters`: Count of GIS clusters
- `GIS_Cluster_Cases`: Count of GIS cases

## Monitoring

- **Cloud Functions Logs**: Execution details and errors
- **Cloud Scheduler**: Job execution status
- **BigQuery**: Table statistics and query history

## API Response

```json
{
  "status": "success",
  "message": "Successfully processed 2025-01-15",
  "date": "2025-01-15",
  "processing_time": "2025-01-15T10:30:00",
  "abc_clusters": 5,
  "abc_cases": 12,
  "gis_clusters": 3,
  "gis_cases": 8,
  "total_clusters": 8,
  "total_cases": 20,
  "clusters": [...]
}
```