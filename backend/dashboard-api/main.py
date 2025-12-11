from flask import Flask, jsonify
from flask_cors import CORS
from google.cloud import bigquery
import os
from datetime import datetime
import requests

app = Flask(__name__)
CORS(app)

# Configuration
PROJECT_ID = os.getenv('PROJECT_ID', 'sentinel-h-5')
DATASET_ID = os.getenv('DATASET_ID', 'sentinel_h_5')
TABLE_ID = os.getenv('TABLE_ID', 'patient_records')

# Column names
PATIENT_ENTRY_DATE = 'patient_entry_date'
LATITUDE = 'latitude'
LONGITUDE = 'longitude'
AREA_TYPE = 'pat_areatype'

client = bigquery.Client(project=PROJECT_ID)

@app.route('/api/dashboard/stats', methods=['GET'])
def get_dashboard_stats():
    try:
        from flask import request
        
        # Get date filter parameter
        days = request.args.get('days')
        
        # Build date filter condition
        date_filter = ""
        if days and days.isdigit():
            date_filter = f"WHERE patient_entry_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)"
        
        # Query for dashboard statistics
        query = f"""
        SELECT 
            COUNT(*) as total_cases,
            COUNT(CASE WHEN UPPER(pat_areatype) = 'URBAN' AND latitude IS NOT NULL THEN 1 END) as geocoded_cases,
            CASE 
                WHEN COUNT(CASE WHEN UPPER(pat_areatype) = 'URBAN' THEN 1 END) > 0 
                THEN ROUND(COUNT(CASE WHEN UPPER(pat_areatype) = 'URBAN' AND latitude IS NOT NULL THEN 1 END) * 100.0 / COUNT(CASE WHEN UPPER(pat_areatype) = 'URBAN' THEN 1 END), 1)
                ELSE 0
            END as geocoded_percentage,
            COUNT(CASE WHEN UPPER(pat_areatype) = 'URBAN' THEN 1 END) as urban_cases,
            COUNT(CASE WHEN UPPER(pat_areatype) = 'RURAL' THEN 1 END) as rural_cases
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        {date_filter}
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            stats = {
                'total_cases': row.total_cases,
                'geocoded_cases': row.geocoded_cases,
                'geocoded_percentage': row.geocoded_percentage,
                'urban_cases': row.urban_cases,
                'rural_cases': row.rural_cases
            }
            break
        
        # Check streaming buffer status
        streaming_status = check_streaming_buffer()
        
        return jsonify({
            'success': True,
            'data': {
                **stats,
                'streaming_status': streaming_status,
                'last_updated': datetime.now().isoformat()
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/geocoding/status', methods=['GET'])
def get_geocoding_status():
    try:
        # Query for detailed geocoding statistics
        query = f"""
        WITH daily_stats AS (
            SELECT 
                DATE(patient_entry_date) as date,
                COUNT(*) as total_patients,
                COUNTIF(latitude IS NOT NULL AND longitude IS NOT NULL 
                        AND latitude != 0 AND longitude != 0 
                        AND UPPER(pat_areatype) = 'URBAN') as geocoded_urban,
                COUNTIF(UPPER(pat_areatype) = 'URBAN') as total_urban,
                CASE 
                    WHEN COUNTIF(UPPER(pat_areatype) = 'URBAN') > 0 
                    THEN COUNTIF(latitude IS NOT NULL AND longitude IS NOT NULL 
                                 AND latitude != 0 AND longitude != 0 
                                 AND UPPER(pat_areatype) = 'URBAN') / COUNTIF(UPPER(pat_areatype) = 'URBAN')
                    ELSE 1.0 
                END as geocoding_pct
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            WHERE patient_entry_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
            GROUP BY 1
        ),
        overall_stats AS (
            SELECT 
                COUNT(*) as total_records,
                COUNTIF(UPPER(pat_areatype) = 'URBAN') as total_urban,
                COUNTIF(latitude IS NOT NULL AND longitude IS NOT NULL 
                        AND latitude != 0 AND longitude != 0 
                        AND UPPER(pat_areatype) = 'URBAN') as geocoded_urban,
                ROUND(COUNTIF(latitude IS NOT NULL AND longitude IS NOT NULL 
                              AND latitude != 0 AND longitude != 0 
                              AND UPPER(pat_areatype) = 'URBAN') / COUNTIF(UPPER(pat_areatype) = 'URBAN') * 100, 2) as urban_geocoding_pct,
                COUNTIF(latitude IS NOT NULL AND longitude IS NOT NULL 
                        AND latitude != 0 AND longitude != 0) as total_geocoded,
                ROUND(COUNTIF(latitude IS NOT NULL AND longitude IS NOT NULL 
                              AND latitude != 0 AND longitude != 0) / COUNT(*) * 100, 2) as overall_geocoding_pct
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        )
        SELECT 
            o.*,
            COUNTIF(d.geocoding_pct >= 0.85) as days_ready,
            COUNT(*) as total_days,
            MIN(d.geocoding_pct) as min_daily_pct,
            AVG(d.geocoding_pct) as avg_daily_pct
        FROM overall_stats o
        CROSS JOIN daily_stats d
        GROUP BY o.total_records, o.total_urban, o.geocoded_urban, o.urban_geocoding_pct, o.total_geocoded, o.overall_geocoding_pct
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            geocoding_status = {
                'total_records': row.total_records,
                'total_urban': row.total_urban,
                'geocoded_urban': row.geocoded_urban,
                'urban_geocoding_pct': float(row.urban_geocoding_pct),
                'total_geocoded': row.total_geocoded,
                'overall_geocoding_pct': float(row.overall_geocoding_pct),
                'days_ready': row.days_ready,
                'total_days': row.total_days,
                'min_daily_pct': float(row.min_daily_pct) * 100,
                'avg_daily_pct': float(row.avg_daily_pct) * 100,
                'clustering_ready': row.days_ready == row.total_days and row.urban_geocoding_pct >= 85.0,
                'threshold': 85.0
            }
            break
        
        return jsonify({
            'success': True,
            'data': geocoding_status
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def check_streaming_buffer():
    try:
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        table = client.get_table(table_ref)
        
        if table.streaming_buffer:
            return {
                'has_buffer': True,
                'estimated_rows': table.streaming_buffer.estimated_rows,
                'estimated_bytes': table.streaming_buffer.estimated_bytes
            }
        else:
            return {
                'has_buffer': False,
                'estimated_rows': 0,
                'estimated_bytes': 0
            }
    except Exception:
        return {
            'has_buffer': False,
            'estimated_rows': 0,
            'estimated_bytes': 0
        }

@app.route('/api/cases', methods=['GET'])
def get_cases():
    try:
        from flask import request
        
        # Get pagination parameters
        limit = request.args.get('limit', 50, type=int)
        offset = request.args.get('offset', 0, type=int)
        
        # Query for cases data with 3-month filter
        query = f"""
        SELECT 
            unique_id,
            patient_entry_date,
            pat_age,
            pat_sex,
            clini_primary_syn,
            site_code,
            statename,
            districtname,
            subdistrictname,
            villagename,
            CONCAT(
                COALESCE(pat_street, ''), ' ',
                COALESCE(villagename, ''), ' ',
                COALESCE(districtname, ''), ' ',
                COALESCE(statename, '')
            ) as complete_address
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE patient_entry_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
        ORDER BY patient_entry_date DESC
        LIMIT {limit} OFFSET {offset}
        """
        
        # Get total count for pagination
        count_query = f"""
        SELECT COUNT(*) as total_count
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE patient_entry_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
        """
        
        query_job = client.query(query)
        count_job = client.query(count_query)
        
        results = query_job.result()
        count_result = list(count_job.result())[0]
        
        cases_data = []
        for row in results:
            cases_data.append({
                'unique_id': row.unique_id,
                'patient_entry_date': str(row.patient_entry_date) if row.patient_entry_date else None,
                'pat_age': row.pat_age,
                'pat_sex': row.pat_sex,
                'clini_primary_syn': row.clini_primary_syn,
                'site_code': row.site_code,
                'statename': row.statename,
                'districtname': row.districtname,
                'subdistrictname': row.subdistrictname,
                'villagename': row.villagename,
                'complete_address': row.complete_address.strip() if row.complete_address else ''
            })
        
        return jsonify({
            'success': True,
            'data': cases_data,
            'total': len(cases_data),
            'total_count': count_result.total_count,
            'offset': offset,
            'limit': limit
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500





@app.route('/api/smart-clusters/stats', methods=['GET'])
def get_smart_cluster_stats():
    try:
        # Query for system overview metrics
        query = f"""
        WITH patient_stats AS (
            SELECT 
                COUNT(*) as total_records,
                MAX(patient_entry_date) as last_uploaded_date
            FROM `{PROJECT_ID}.{DATASET_ID}.patient_records`
        ),
        cluster_stats AS (
            SELECT 
                COUNT(DISTINCT smart_cluster_id) as total_clusters,
                MAX(original_creation_date) as last_analysed_date
            FROM `{PROJECT_ID}.{DATASET_ID}.smart_clusters`
        ),
        pending_stats AS (
            SELECT 
                COUNT(*) as total_pending
            FROM (
                SELECT DISTINCT patient_entry_date as date
                FROM `{PROJECT_ID}.{DATASET_ID}.patient_records`
                WHERE patient_entry_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            ) all_dates
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.smart_processing_status` sps
            ON all_dates.date = sps.date AND sps.status = 'COMPLETED'
            WHERE sps.date IS NULL
        )
        SELECT 
            p.total_records,
            c.total_clusters,
            p.last_uploaded_date,
            c.last_analysed_date,
            pd.total_pending
        FROM patient_stats p
        CROSS JOIN cluster_stats c
        CROSS JOIN pending_stats pd
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            stats = {
                'total_records': row.total_records or 0,
                'total_clusters': row.total_clusters or 0,
                'last_uploaded_date': str(row.last_uploaded_date) if row.last_uploaded_date else 'N/A',
                'last_analysed_date': str(row.last_analysed_date) if row.last_analysed_date else 'N/A',
                'total_pending': row.total_pending or 0
            }
            break
        else:
            stats = {
                'total_records': 0,
                'total_clusters': 0,
                'last_uploaded_date': 'N/A',
                'last_analysed_date': 'N/A',
                'total_pending': 0
            }
        
        return jsonify({
            'success': True,
            'data': stats
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'})

@app.route('/api/health/services', methods=['GET'])
def check_services_health():
    health_status = {}
    
    # Upload Function - check by testing BigQuery connection
    try:
        query = f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` LIMIT 1"
        query_job = client.query(query)
        results = list(query_job.result())
        health_status['Upload Function'] = 'healthy' if results else 'down'
    except:
        health_status['Upload Function'] = 'down'
    
    # BigQuery Sync - same as upload since they use same service
    health_status['BigQuery Sync'] = health_status['Upload Function']
    
    # Geocoding Service - check by testing function endpoint
    try:
        import requests
        response = requests.get('https://geocode-addresses-196547645490.asia-south1.run.app', timeout=5)
        health_status['Geocoding Service'] = 'healthy' if response.status_code in [200, 404] else 'down'
    except:
        health_status['Geocoding Service'] = 'down'
    
    # Dashboard API - self
    health_status['Dashboard API'] = 'healthy'
    
    return jsonify(health_status)

@app.route('/api/geocoding/progress', methods=['GET'])
def get_geocoding_progress():
    try:
        query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNTIF(latitude IS NOT NULL AND longitude IS NOT NULL) as geocoded_records,
            CASE 
                WHEN COUNT(*) > 0 
                THEN ROUND(COUNTIF(latitude IS NOT NULL AND longitude IS NOT NULL) / COUNT(*) * 100, 1)
                ELSE 0
            END as completion_pct
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE UPPER(COALESCE(pat_areatype, '')) = 'URBAN'
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        progress = {'total_records': 0, 'geocoded_records': 0, 'completion_pct': 0}
        
        for row in results:
            progress = {
                'total_records': int(row.total_records or 0),
                'geocoded_records': int(row.geocoded_records or 0),
                'completion_pct': float(row.completion_pct or 0)
            }
            break
        
        return jsonify({
            'success': True,
            'data': progress
        })
        
    except Exception as e:
        print(f"Geocoding progress error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)