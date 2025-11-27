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
        # Query smart clusters statistics for last 14 days from max date
        query = f"""
        WITH max_date AS (
            SELECT MAX(original_creation_date) as max_date
            FROM `{PROJECT_ID}.{DATASET_ID}.smart_clusters`
        )
        SELECT 
            COUNT(*) as total_clusters,
            COUNTIF(accept_status = 'Accepted') as accepted_clusters,
            COUNTIF(accept_status = 'Pending') as pending_clusters,
            SUM(patient_count) as total_patients
        FROM `{PROJECT_ID}.{DATASET_ID}.smart_clusters` s
        CROSS JOIN max_date m
        WHERE s.original_creation_date >= DATE_SUB(m.max_date, INTERVAL 14 DAY)
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            stats = {
                'accepted_clusters': row.accepted_clusters or 0,
                'pending_clusters': row.pending_clusters or 0,
                'total_patients': row.total_patients or 0
            }
            break
        else:
            stats = {
                'accepted_clusters': 0,
                'pending_clusters': 0,
                'total_patients': 0
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

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)