import requests
import json

SMART_CLUSTER_API = 'https://smart-cluster-engine-196547645490.asia-south1.run.app'

def test_cluster_accept():
    # First, get all clusters to find a pending one
    print("1. Getting all clusters...")
    response = requests.get(f'{SMART_CLUSTER_API}/smart-clusters')
    data = response.json()
    
    if not data.get('success'):
        print(f"Failed to get clusters: {data}")
        return
    
    clusters = data.get('clusters', [])
    pending_clusters = [c for c in clusters if c.get('accept_status') == 'Pending']
    
    print(f"Found {len(pending_clusters)} pending clusters")
    
    if not pending_clusters:
        print("No pending clusters found to test")
        return
    
    # Test with first pending cluster
    test_cluster = pending_clusters[0]
    cluster_id = test_cluster['smart_cluster_id']
    
    print(f"\n2. Testing with cluster: {cluster_id}")
    print(f"   Current status: {test_cluster['accept_status']}")
    
    # Accept the cluster
    print(f"\n3. Accepting cluster {cluster_id}...")
    accept_response = requests.post(
        f'{SMART_CLUSTER_API}/accept-cluster',
        headers={'Content-Type': 'application/json'},
        json={'cluster_id': cluster_id}
    )
    
    accept_data = accept_response.json()
    print(f"Accept response: {accept_data}")
    
    if not accept_data.get('success'):
        print(f"Failed to accept cluster: {accept_data}")
        return
    
    # Check if status changed
    print(f"\n4. Checking cluster status after accept...")
    response = requests.get(f'{SMART_CLUSTER_API}/smart-clusters')
    data = response.json()
    
    if data.get('success'):
        updated_clusters = data.get('clusters', [])
        updated_cluster = next((c for c in updated_clusters if c['smart_cluster_id'] == cluster_id), None)
        
        if updated_cluster:
            print(f"   Updated status: {updated_cluster['accept_status']}")
            if updated_cluster['accept_status'] == 'Accepted':
                print("✅ SUCCESS: Cluster status updated to Accepted")
            else:
                print(f"❌ FAILED: Status is still {updated_cluster['accept_status']}")
        else:
            print("❌ FAILED: Cluster not found after accept")
    else:
        print(f"Failed to get updated clusters: {data}")

if __name__ == "__main__":
    test_cluster_accept()