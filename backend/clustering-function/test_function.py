#!/usr/bin/env python3
"""Test script for clustering function"""

import requests
import json

def test_clustering_function():
    """Test the clustering function locally or deployed"""
    
    # For local testing with functions-framework
    # Run: functions-framework --target=cluster_analysis --debug
    LOCAL_URL = "http://localhost:8080"
    
    # For deployed function
    DEPLOYED_URL = "https://asia-south1-sentinel-h-5.cloudfunctions.net/cluster-analysis"
    
    # Test payload
    test_data = {
        "time_window": 7,
        "min_cases": 2
    }
    
    try:
        # Test deployed function
        print("Testing deployed function...")
        response = requests.post(DEPLOYED_URL, json=test_data, timeout=600)
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Function executed successfully!")
            print(f"Status: {result.get('status')}")
            print(f"Message: {result.get('message')}")
            print(f"Date processed: {result.get('date')}")
            print(f"ABC clusters: {result.get('abc_clusters')}")
            print(f"GIS clusters: {result.get('gis_clusters')}")
            print(f"Total cases: {result.get('total_cases')}")
        else:
            print(f"❌ Function failed with status: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ Error testing function: {str(e)}")

if __name__ == "__main__":
    test_clustering_function()