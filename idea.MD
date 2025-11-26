# Cluster Merge Enhancement Idea

## Problem Statement
Current system creates artificial fragmentation where the same outbreak (same village + same syndrome) gets split into multiple cluster IDs across different days, losing outbreak continuity and making analysis difficult.

## Current Behavior
- Day 1: Village-A + Fever = Cluster-1 [a,b,c,d]
- Day 4: Village-A + Fever = Cluster-2 [c,d,e,f,g]
- Result: Same outbreak treated as separate clusters

## Proposed Solution

### Core Logic
1. **Remove overlap deduplication check** during initial clustering
2. **Implement time-based merge criteria**: If difference between Max and Min reported dates in clusters ≤ 6 days, allow merge
3. **Smart merge process**: Keep original cluster ID, remove overlaps, add only new cases

### Implementation Steps

#### 1. Merge Detection
```
For each new cluster:
  Find existing clusters with same village + same syndrome
  Check: (Max_date - Min_date) ≤ 6 days
  Check: Cluster age ≤ 20 days (from original creation)
  If both true: Trigger merge process
```

#### 2. Merge Process
```
Keep: Original cluster ID (older cluster)
Remove: Overlap patients from new cluster
Add: Non-overlap patients to original cluster
Mark: New patients with current timestamp as "newly added"
Archive: New cluster as "Merged_Into_[original_id]"
```

#### 3. Data Updates
- **Original Cluster**: Update patient_count, set merge_status = 'Expanded'
- **New Assignments**: Move non-overlap patients to original cluster_id
- **Timestamps**: Preserve original assigned_at for existing, new assigned_at for additions
- **Audit Trail**: Log merge decision in merge_history table

## Simulation Example

### Scenario
- **Day 1**: Cluster-1 = [a,b,c,d] (Village-A + Fever)
- **Day 4**: New cluster = [c,d,e,f,g] (Village-A + Fever)

### Merge Check
- Max date = Day 4, Min date = Day 1
- Difference = 3 days < 6 days ✅
- Overlap = [c,d]
- New cases = [e,f,g]

### Result
- **Cluster-1** = [a,b,c,d,e,f,g] (7 patients total)
- **Original cases** [a,b,c,d]: Keep Day 1 timestamp
- **New cases** [e,f,g]: Get Day 4 timestamp
- **Status**: merge_status = 'Expanded'

## Benefits

1. **Outbreak Continuity**: Single cluster ID tracks entire outbreak lifecycle
2. **Growth Timeline**: Clear distinction between original and newly added cases
3. **No Duplicate Counting**: Overlap patients counted only once
4. **Epidemiological Accuracy**: Reflects real-world outbreak progression
5. **Simple Analysis**: One cluster = one outbreak
6. **Audit Trail**: Complete history of cluster evolution

## Technical Advantages

- **No Schema Changes**: Works with existing table structure
- **Backward Compatible**: Existing clusters remain unchanged
- **Clear Timestamps**: assigned_at distinguishes case addition timeline
- **Flexible Threshold**: 6-day window can be adjusted based on disease characteristics
- **Preserves History**: Original cluster creation date maintained

## GIS Clustering Extension

### Core Logic for GIS Clusters
Same merge enhancement applies to GIS (urban) clusters with geographic proximity criteria:

#### Merge Criteria
```
Same syndrome + Geographic proximity + Time window ≤ 6 days
Geographic proximity = Distance between centroids ≤ 500m (DBSCAN_EPSILON_M)
```

#### Detection Process
```
For each new GIS cluster:
  Find existing GIS clusters with:
    - Same primary_syndrome
    - Centroid distance ≤ 500m (haversine formula)
    - (Max_date - Min_date) ≤ 6 days
    - Cluster age ≤ 20 days (from original creation)
    - Projected radius after merge ≤ 800m
  If all criteria met: Trigger merge
```

#### GIS Merge Process
- **Keep**: Original cluster_id (older cluster)
- **Update**: Centroid coordinates (weighted average by patient count)
- **Update**: actual_cluster_radius (recalculate from new centroid)
- **Remove**: Overlap patients from new cluster
- **Add**: Non-overlap patients to original cluster

### GIS Scenarios

#### Scenario 1: Same Location Outbreak
- **Day 1**: GIS-001 = [P1,P2,P3] at (19.0760, 72.8777) - Fever
- **Day 3**: New = [P2,P3,P4,P5] at (19.0765, 72.8780) - Fever
- **Check**: 50m < 500m ✅, 2 days < 6 days ✅
- **Result**: Merge → GIS-001 = [P1,P2,P3,P4,P5]

#### Scenario 2: Different Locations
- **Day 1**: GIS-001 at (19.0760, 72.8777)
- **Day 4**: New cluster at (19.1200, 72.9500)
- **Check**: 8.2km > 500m ❌
- **Result**: Separate clusters

#### Scenario 3: Time Window Exceeded
- **Day 1**: GIS-001 at (19.0760, 72.8777)
- **Day 8**: New cluster at (19.0762, 72.8775)
- **Check**: 30m < 500m ✅, 7 days > 6 days ❌
- **Result**: Separate clusters

### Merge Constraints

#### Prevent Runaway Growth
- **Maximum Cluster Age**: 20 days from original cluster creation (both ABC & GIS)
- **Geographic Expansion Limit**: GIS clusters cannot exceed 800m radius after merge
- **Time Window**: 6-day window between min/max case dates

#### Constraint Examples
- **Age Limit**: Day 1 cluster stops merging after Day 21
- **Radius Limit**: If merge would create 850m radius → No merge
- **Time Gap**: If case dates span >6 days → No merge

### Algorithm Comparison
- **ABC Clusters**: Same village + same syndrome + time window + age limit
- **GIS Clusters**: Geographic proximity (≤500m) + same syndrome + time window + age limit + radius limit
- **Both**: Use identical 6-day time window and 20-day age limit for outbreak continuity

## Implementation Priority
**High** - Addresses core epidemiological tracking requirements and eliminates artificial cluster fragmentation for both rural (ABC) and urban (GIS) outbreak detection.