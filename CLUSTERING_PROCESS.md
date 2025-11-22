# Sentinel-5 Clustering Process Documentation

## Overview

The Sentinel-5 clustering system automatically detects disease outbreaks by grouping patient cases using two algorithms:
- **ABC Clustering**: Area-Based Clustering for rural areas (groups by village + syndrome)
- **GIS Clustering**: Geographic clustering for urban areas (uses DBSCAN with 350m radius)

## System Architecture

```
Patient Data → Geocoding (90%+) → Clustering Analysis → Smart Matching → Status Assignment
```

## Core Components

### 1. Data Selection Criteria
- **Geocoding Threshold**: ≥90% of cases must have latitude/longitude
- **Time Window**: Analyzes 7 days of patient data
- **Minimum Cases**: Requires ≥2 cases to form a cluster
- **Area Types**: Rural (ABC) vs Urban (GIS) clustering

### 2. Smart Cluster Matching
When new clusters are detected, the system checks against recent clusters (14-day lookback):

| Distance | Status | Action |
|----------|--------|--------|
| ≤50m | `ACCEPTED` | Auto-approved, immediate processing |
| 50-150m | `PENDING_MERGE` | Requires human review (merge decision) |
| >150m | `PENDING_NEW` | Requires human review (new outbreak?) |

### 3. Auto-Accept Mechanism
- **Timeout**: 3 days (`AUTO_ACCEPT_TIMEOUT_DAYS=3`)
- **Action**: Automatically changes `PENDING_MERGE` and `PENDING_NEW` to `ACCEPTED`
- **Purpose**: Prevents indefinite blocking while maintaining data quality

## Detailed Process Flow

### Step 1: Eligibility Check
```sql
-- Find dates with sufficient geocoding coverage
SELECT patient_entry_date, 
       COUNT(*) as total_cases,
       COUNT(latitude) * 100.0 / COUNT(*) as geocoding_pct
FROM patient_records 
GROUP BY patient_entry_date
HAVING geocoding_pct >= 90
ORDER BY patient_entry_date ASC
```

### Step 2: Blocking Check
```sql
-- Check for pending clusters that block processing
SELECT 1 FROM temp_cluster_table
WHERE accept_status IN ('PENDING_MERGE', 'PENDING_NEW')
  AND analysis_input_date >= @target_date - 3
```

### Step 3: Auto-Accept Expired Clusters
```sql
-- Auto-accept clusters older than 3 days
UPDATE temp_cluster_table 
SET accept_status = 'ACCEPTED'
WHERE accept_status IN ('PENDING_MERGE', 'PENDING_NEW')
  AND analysis_input_date <= @target_date - 3
```

### Step 4: ABC Clustering (Rural Areas)
```python
# Group by location + syndrome
groups = defaultdict(list)
for case in rural_cases:
    key = (statename, districtname, villagename, syndrome)
    groups[key].append(case)

# Create clusters for groups with ≥2 cases
for location_syndrome, cases in groups.items():
    if len(cases) >= 2:
        centroid = calculate_centroid(cases)
        cluster_id = generate_cluster_id('ABC', location, syndrome, date, seq)
```

### Step 5: GIS Clustering (Urban Areas)
```python
# DBSCAN clustering with 350m radius
from sklearn.cluster import DBSCAN
coords = [[case.lat, case.lon] for case in urban_cases]
clusters = DBSCAN(eps=350/6371000, min_samples=2, metric='haversine').fit(coords)

# Process each cluster
for cluster_label in unique_labels:
    cluster_cases = cases[labels == cluster_label]
    centroid = calculate_centroid(cluster_cases)
    cluster_id = generate_cluster_id('GIS', location, syndrome, date, seq)
```

### Step 6: Cluster ID Generation
```python
def generate_cluster_id(algo_type, location_code, syndrome, date_str, seq):
    # Clean syndrome and create 3-char code (first + middle + last letters)
    clean_syn = "".join(c for c in syndrome if c.isalnum()).upper()
    if len(clean_syn) >= 3:
        syn_code = clean_syn[0] + clean_syn[len(clean_syn)//2] + clean_syn[-1]
    
    return f"{algo_type}_{location_code}_{syn_code}_{date_str}_{seq:03d}"
```

**Examples:**
- `"Acute Fever"` → `"ACUTEFEVER"` → `"AFR"`
- `"Chronic Pain"` → `"CHRONICPAIN"` → `"CIN"`
- `"High Blood Pressure"` → `"HIGHBLOODPRESSURE"` → `"HRE"`

## Complete Example Walkthrough

### Scenario: 5-Day Processing Gap

**Initial State (Nov 22, 2025):**
```json
{"status": "success", "analysis_input_date": "2025-09-23", "total_clusters": 48, "total_cases": 205}
```

**Day 1-3: System Idle**
- No processing occurs
- Existing clusters remain in various states

**Day 4 (Nov 26): Curl Request**
```bash
curl -X POST https://asia-south1-sentinel-h-5.cloudfunctions.net/cluster-analysis-v2
```

**System Processing:**

1. **Check Streaming Buffer**: ✅ Clear
2. **Get Processed Dates**: `["2025-09-23"]` from cluster_summary_table
3. **Find Next Eligible Date**:
   ```
   2025-09-24: 75% geocoded → SKIP
   2025-09-25: 82% geocoded → SKIP  
   2025-09-26: 94% geocoded → SELECT
   ```
4. **Auto-Accept Expired Clusters**: None (no clusters >3 days old)
5. **Check Blocking**: No pending clusters found
6. **Process 2025-09-26**:
   - ABC: 15 rural clusters (12 ACCEPTED, 2 PENDING_MERGE, 1 PENDING_NEW)
   - GIS: 8 urban clusters (6 ACCEPTED, 1 PENDING_MERGE, 1 PENDING_NEW)

**Response:**
```json
{
  "status": "success",
  "analysis_input_date": "2025-09-26", 
  "total_clusters": 23,
  "total_cases": 156
}
```

**Day 5 (Nov 27): Another Curl Request**
```bash
curl -X POST https://asia-south1-sentinel-h-5.cloudfunctions.net/cluster-analysis-v2
```

**System Processing:**
1. **Get Processed Dates**: `["2025-09-23", "2025-09-26"]`
2. **Find Next Eligible Date**: `2025-09-27` (91% geocoded)
3. **Check Blocking**: 
   ```
   Found PENDING clusters from 2025-09-26 (1 day old < 3 day timeout)
   ```

**Response:**
```json
{
  "status": "blocked",
  "message": "Pending human review (auto-accepts after 3 days)",
  "blocked_date": "2025-09-27"
}
```

**Day 8 (Nov 30): Curl After Auto-Accept Timeout**
```bash
curl -X POST https://asia-south1-sentinel-h-5.cloudfunctions.net/cluster-analysis-v2
```

**System Processing:**
1. **Auto-Accept Expired**: Updates 2025-09-26 pending clusters to ACCEPTED
2. **Find Next Eligible Date**: `2025-09-27` (91% geocoded)
3. **Check Blocking**: No pending clusters (all auto-accepted)
4. **Process 2025-09-27**: Creates new clusters

**Response:**
```json
{
  "status": "success",
  "analysis_input_date": "2025-09-27",
  "total_clusters": 31,
  "total_cases": 189
}
```

## Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `GEOCODING_THRESHOLD` | 90 | Minimum % of geocoded cases required |
| `TIME_WINDOW` | 7 | Days of patient data to analyze |
| `MIN_CASES` | 2 | Minimum cases required to form cluster |
| `GIS_EPS_METERS` | 350 | DBSCAN radius for urban clustering |
| `AUTO_ACCEPT_TIMEOUT_DAYS` | 3 | Days before auto-accepting pending clusters |

## Database Tables

### temp_cluster_table
**Purpose**: Stores all cluster analysis results
**Partitioning**: By `analysis_input_date` (daily partitions)
**Clustering**: By `accept_status`, `algorithm_type`, `statename`, `districtname`

**Key Fields:**
- `cluster_id`: Unique identifier (e.g., `ABC_MHPN_AFR_23NOV2025_001`)
- `accept_status`: `ACCEPTED`, `PENDING_MERGE`, `PENDING_NEW`, `REJECTED`
- `algorithm_type`: `ABC` or `GIS`
- `cluster_centroid_lat/lon`: Geographic center of cluster
- `cluster_case_count`: Number of cases in cluster

### cluster_summary_table  
**Purpose**: Tracks processing history and statistics
**Partitioning**: By `analysis_input_date`

**Key Fields:**
- `analysis_input_date`: Date of patient data processed
- `cluster_analysis_run_date`: When analysis was performed
- `accepted_abc_clusters`: Count of accepted ABC clusters
- `accepted_gis_clusters`: Count of accepted GIS clusters

## API Responses

### Success Response
```json
{
  "status": "success",
  "analysis_input_date": "2025-09-26",
  "total_clusters": 23,
  "total_cases": 156
}
```

### Blocked Response
```json
{
  "status": "blocked", 
  "message": "Pending human review (auto-accepts after 3 days)",
  "blocked_date": "2025-09-27"
}
```

### No Data Response
```json
{
  "status": "no_data",
  "message": "No new eligible date"
}
```

### Streaming Buffer Response
```json
{
  "status": "skipped",
  "message": "Streaming buffer active"
}
```

## Manual Interventions

### Accept All Pending Clusters
```sql
UPDATE `sentinel-h-5.sentinel_h_5.temp_cluster_table`
SET accept_status = 'ACCEPTED'
WHERE accept_status IN ('PENDING_MERGE', 'PENDING_NEW');
```

### Reject Specific Clusters
```sql
UPDATE `sentinel-h-5.sentinel_h_5.temp_cluster_table`
SET accept_status = 'REJECTED'
WHERE cluster_id = 'ABC_MHPN_AFR_23NOV2025_001';
```

### Check Processing Status
```sql
SELECT analysis_input_date, 
       accepted_abc_clusters + accepted_gis_clusters as total_accepted,
       total_abc_cases + total_gis_cases as total_cases
FROM `sentinel-h-5.sentinel_h_5.cluster_summary_table`
ORDER BY analysis_input_date DESC
LIMIT 10;
```

### Review Pending Clusters with All Candidate Matches
```sql
SELECT 
  cluster_id,
  accept_status,
  algorithm_type,
  matched_cluster_id as closest_match,
  match_distance_meters as closest_distance,
  match_confidence_score,
  candidate_clusters as all_candidates,
  cluster_case_count,
  CASE 
    WHEN match_confidence_score >= 80 THEN 'HIGH - Likely same outbreak'
    WHEN match_confidence_score >= 40 THEN 'MEDIUM - Possible connection'
    WHEN match_confidence_score >= 15 THEN 'LOW - Unlikely connection'
    ELSE 'VERY LOW - Probably new outbreak'
  END as recommendation,
  statename,
  districtname,
  villagename,
  clini_primary_syn
FROM `sentinel-h-5.sentinel_h_5.temp_cluster_table`
WHERE accept_status IN ('PENDING_MERGE', 'PENDING_NEW')
ORDER BY match_confidence_score DESC, cluster_case_count DESC;
```

### Example Output for Multiple Candidates
```
cluster_id: GIS_MHPN_AFR_24SEP2025_003
closest_match: GIS_MHPN_AFR_23SEP2025_001
closest_distance: 87.5m
all_candidates: [
  {"cluster_id": "GIS_MHPN_AFR_23SEP2025_001", "distance_m": 87.5},
  {"cluster_id": "GIS_MHPN_AFR_23SEP2025_005", "distance_m": 124.3},
  {"cluster_id": "ABC_MHPN_AFR_22SEP2025_002", "distance_m": 143.8}
]
recommendation: "MEDIUM - Possible connection"
```

## Monitoring and Troubleshooting

### Check Geocoding Coverage
```sql
SELECT patient_entry_date,
       COUNT(*) as total_records,
       COUNT(latitude) as geocoded_records,
       COUNT(latitude) * 100.0 / COUNT(*) as geocoding_pct
FROM `sentinel-h-5.sentinel_h_5.patient_records`
WHERE patient_entry_date >= '2025-09-01'
GROUP BY patient_entry_date
ORDER BY patient_entry_date DESC;
```

### View Pending Clusters
```sql
SELECT cluster_id, accept_status, algorithm_type, 
       cluster_case_count, analysis_input_date
FROM `sentinel-h-5.sentinel_h_5.temp_cluster_table`
WHERE accept_status IN ('PENDING_MERGE', 'PENDING_NEW')
ORDER BY analysis_input_date DESC;
```

### Function Logs
```bash
gcloud functions logs read cluster-analysis-v2 --region=asia-south1 --limit=10
```

## Deployment

### Function URL
```
https://asia-south1-sentinel-h-5.cloudfunctions.net/cluster-analysis-v2
```

### Scheduler
- **Frequency**: Every 30 minutes
- **Timezone**: Asia/Kolkata
- **Job Name**: clustering-scheduler-v2

### Environment Variables
```bash
PROJECT_ID=sentinel-h-5
DATASET_ID=sentinel_h_5
TABLE_ID=patient_records
TIME_WINDOW=7
MIN_CASES=2
GEOCODING_THRESHOLD=90
GIS_EPS_METERS=350
TEMP_CLUSTER_TABLE=temp_cluster_table
CLUSTER_SUMMARY_TABLE=cluster_summary_table
AUTO_ACCEPT_TIMEOUT_DAYS=3
```

## Performance Characteristics

- **Processing Speed**: ~2-5 minutes per date (depending on case volume)
- **Daily Capacity**: Unlimited (processes one date per run)
- **Memory Usage**: 2GB allocated, ~500MB typical usage
- **Timeout**: 540 seconds (9 minutes)
- **Concurrency**: 1 (prevents race conditions)

## Security Features

- **IAM**: Uses default compute service account
- **Data Access**: Read/write to BigQuery only
- **Network**: HTTPS only, no external API calls
- **Logging**: Comprehensive audit trail in Cloud Functions logs