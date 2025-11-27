# Cloud Run Services

## Smart Cluster Engine

**Service URL**: https://smart-cluster-engine-196547645490.asia-south1.run.app

### Endpoints

- **GET** `/` - Service info and endpoint list
- **GET** `/health` - Combined health check, system status, and clustering statistics
- **POST** `/smart-process` - Process next date with smart clustering
- **GET/POST** `/smart-preflight` - Combined data quality check and test clustering
- **POST** `/smart-init` - Initialize smart clustering tables
- **POST** `/smart-batch` - Process multiple dates
- **GET/POST** `/smart-config` - Get/update clustering configuration
- **GET** `/smart-clusters` - Get smart clusters with optional date filtering
- **GET** `/smart-cluster-patients` - Get patients in a specific cluster
- **GET** `/maps-api-key` - Get Google Maps API key from Secret Manager
- **POST** `/accept-cluster` - Accept a pending cluster
- **POST** `/reject-cluster` - Reject and delete a pending cluster
- **POST** `/smart-truncate` - Truncate all smart clustering tables

### Usage Examples

**Health Check**:
```bash
curl https://smart-cluster-engine-196547645490.asia-south1.run.app/health
```

**Process Next Date**:
```bash
curl -X POST https://smart-cluster-engine-196547645490.asia-south1.run.app/smart-process
```

**Data Quality Check**:
```bash
curl https://smart-cluster-engine-196547645490.asia-south1.run.app/smart-preflight
```

**Get Clusters**:
```bash
curl https://smart-cluster-engine-196547645490.asia-south1.run.app/smart-clusters
```

**Accept Cluster**:
```bash
curl -X POST https://smart-cluster-engine-196547645490.asia-south1.run.app/accept-cluster \
  -H "Content-Type: application/json" \
  -d '{"cluster_id": "SMART-GIS-1234567890-abcd1234"}'
```