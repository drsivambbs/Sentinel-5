from flask import Flask, jsonify
from flask_cors import CORS
from google.cloud import bigquery
import os
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Configuration
PROJECT_ID = os.getenv('PROJECT_ID', 'sentinel-h-5')
DATASET_ID = os.getenv('DATASET_ID', 'sentinel_h_5')
TABLE_ID = os.getenv('TABLE_ID', 'patient_records')

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
            COUNT(latitude) as geocoded_cases,
            ROUND(COUNT(latitude) * 100.0 / COUNT(*), 1) as geocoded_percentage,
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
        
        # Query for cases data
        query = f"""
        SELECT 
            unique_id,
            patient_entry_date,
            pat_age,
            pat_sex,
            clini_primary_syn,
            site_code,
            CONCAT(
                COALESCE(pat_street, ''), ' ',
                COALESCE(villagename, ''), ' ',
                COALESCE(districtname, ''), ' ',
                COALESCE(statename, '')
            ) as complete_address
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        ORDER BY patient_entry_date DESC
        LIMIT {limit} OFFSET {offset}
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        cases_data = []
        for row in results:
            cases_data.append({
                'unique_id': row.unique_id,
                'patient_entry_date': str(row.patient_entry_date) if row.patient_entry_date else None,
                'pat_age': row.pat_age,
                'pat_sex': row.pat_sex,
                'clini_primary_syn': row.clini_primary_syn,
                'site_code': row.site_code,
                'complete_address': row.complete_address.strip() if row.complete_address else ''
            })
        
        return jsonify({
            'success': True,
            'data': cases_data,
            'total': len(cases_data)
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
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))