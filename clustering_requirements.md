# Clustering Function Requirements

## Overview
Implement 2 types of clustering algorithms for patient data analysis.

## Pre-conditions
1. **Geocoding Threshold**: Minimum 90% of cases in any `patient_entry_date` must be geocoded (have latitude/longitude)
2. **Date Selection Logic**:
   - Start with the most recent `patient_entry_date`
   - If 90% geocoded criteria not met, go to previous day
   - Continue backwards until finding a date meeting the 90% threshold
   - Perform clustering only on the selected date

## Clustering Types
- Type 1: Area_Based_Clustering (ABC)
- Type 2: GIS_Based_Clustering (GIS)

## Clustering Execution
- Both clustering algorithms should be executed together on the selected date

## Area_Based_Clustering (ABC) Parameters
- **Area Type Filter**: Only consider records where `pat_areatype` == 'Rural'
- **Village Filter**: Exclude records where `villagename` is NULL
- **Time Window**: Past N days from `patient_entry_date` (excluding the entry date itself)
  - Default: 7 days (configurable input parameter)
- **Grouping**: Group by `clini_primary_syn`
- **Geographic Hierarchy**: `statename` → `districtname` → `subdistrictname` → `villagename`
- **Cluster Definition**: Any village with N+ cases of same `clini_primary_syn` within time window = cluster
  - Default: 2 cases (configurable input parameter)

## GIS_Based_Clustering Parameters
- **Area Type Filter**: Only consider records where `pat_areatype` == 'Urban'
- **Time Window**: Past N days from `patient_entry_date` (excluding the entry date itself)
  - Default: 7 days (configurable input parameter)
- **Grouping**: Group by `clini_primary_syn`
- **Geographic Clustering**: Minimum 500 meter radius using Haversine distance formula
- **Cluster Definition**: N+ cases of same `clini_primary_syn` within 500m radius and time window
  - Default: 2 cases (configurable input parameter)

## Cluster ID Generation
- **Format**: `{TYPE}_{LOCATION}_{DATE}_{SEQUENCE}`
- **Type Codes**:
  - `ABC` = Area_Based_Clustering
  - `GIS` = GIS_Based_Clustering
- **Location Code**: First letter of State-District-Subdistrict-Village
  - Example: Gujarat-Andora-Karakanch-Aloka → `GAKA`
  - **NULL Handling**: Drop alphabet if any field is NULL/None
    - Example: Gujarat-Andora-NULL-Aloka → `GAA`
- **Date Format**: `DDMMMYYYY` (e.g., `15NOV2025`)
- **Sequence**: 3-digit number starting from `001` for multiple clusters in same village/day
- **Examples**:
  - `ABC_GAKA_15NOV2025_001` (first area-based cluster)
  - `ABC_GAKA_15NOV2025_002` (second area-based cluster same village/day)
  - `GIS_GAKA_15NOV2025_001` (first GIS-based cluster)

## Output Storage
- **Table 1**: `temp_cluster_table` (single persistent table)
  - **Content**: Each case in a cluster gets assigned its `cluster_id`
  - **Structure**: One row per case with corresponding cluster_id and `accept_status` (empty column)
  - **Deduplication**: Use `dummy_id` = `cluster_id` + `unique_id` to prevent duplicate entries
  - **Append Mode**: Add new clusters to existing table
- **Table 2**: `cluster_summary_table`
  - **Columns**: Date, Total_ABC_Clusters, ABC_Cluster_Cases, Total_GIS_Clusters, GIS_Cluster_Cases
  - **Purpose**: Summary statistics per processed date

## Processing Control
- **Single Date Processing**: Process only one date per execution cycle
- **Cloud Scheduler**: 30-minute intervals for automated execution
- **Duplicate Prevention**: Check if date already exists in `cluster_summary_table` before processing
- **Date Range Limit**: Stop processing when reaching 15 days before max date in `patient_records` table
  - Example: If max date is 15th, stop processing at 1st of same month
  - **Analysis Window**: Even when stopping at 1st, still consider previous 7 days for clustering analysis
- **Processing Order**: Start from most recent date, work backwards until limit reached

## Error Handling
- If no eligible dates found (no date meets 90% geocoded threshold), log: 'No eligible Date found'
- Skip dates already processed (present in cluster_summary_table)

## Function Output
- **Return Format**: Flat table as JSON
- **Content**: All cluster results for the processed date

## Notes
- Only process dates with sufficient geocoded data
- Ensure meaningful clustering analysis with quality location data