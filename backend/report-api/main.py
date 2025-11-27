import functions_framework
from google.cloud import bigquery
from datetime import datetime, timedelta
import json
import os

PROJECT_ID = os.getenv('PROJECT_ID', 'sentinel-h-5')
DATASET_ID = os.getenv('DATASET_ID', 'sentinel_h_5')
TABLE_ID = os.getenv('TABLE_ID', 'patient_records')

@functions_framework.http
def get_hebs_data(request):
    """API endpoint to fetch HEBS-DSR report data"""
    
    # Enable CORS
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST',
            'Access-Control-Allow-Headers': 'Content-Type',
        }
        return ('', 204, headers)
    
    headers = {'Access-Control-Allow-Origin': '*'}
    
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        
        # Get date range from request or use default (last 7 days)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)
        
        if request.method == 'POST':
            data = request.get_json()
            if data and 'start_date' in data:
                start_date = datetime.strptime(data['start_date'], '%Y-%m-%d').date()
            if data and 'end_date' in data:
                end_date = datetime.strptime(data['end_date'], '%Y-%m-%d').date()
        
        # Fetch case summary counts
        summary_data = get_case_summary(bq_client, start_date, end_date)
        
        # Fetch signals (clustered cases)
        signals_data = get_signals_data(bq_client, start_date, end_date)
        
        response_data = {
            'report_id': f"EBS_DSR-{end_date.strftime('%Y%m%d')}-001",
            'report_date': end_date.strftime('%d.%m.%Y'),
            'start_date': start_date.strftime('%d.%m.%Y'),
            'end_date': end_date.strftime('%d.%m.%Y'),
            'summary': summary_data,
            'signals': signals_data
        }
        
        return (json.dumps(response_data), 200, headers)
        
    except Exception as e:
        return (json.dumps({'error': str(e)}), 500, headers)

def get_case_summary(bq_client, start_date, end_date):
    """Get case counts by syndrome for summary section"""
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Use available symptom fields to detect syndromes
    query = f"""
        SELECT 
            COUNTIF(
                sym_fever = true AND 
                (sym_headache = true OR sym_myalgia = true OR sym_chills = true)
            ) as aufi_count,
            COUNTIF(sym_diarrhea = true) as add_count,
            COUNTIF(
                sym_fever = true AND 
                (sym_cough = true OR sym_breathlessness = true OR sym_sore_throat = true)
            ) as sari_count,
            COUNTIF(
                sym_fever = true AND 
                (sym_altered_sensorium = true OR sym_new_onset_of_seizures = true OR sym_neck_rigidity = true)
            ) as aes_count,
            COUNTIF(
                sym_jaundice = true OR sym_yellow_skin = true OR sym_yellow_sclera = true
            ) as jaundice_count,
            COUNTIF(
                sym_fever = true AND sym_papule_rash = true
            ) as fever_rash_count
        FROM `{table_id}`
        WHERE DATE(pat_date_admission) BETWEEN @start_date AND @end_date
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date)
        ]
    )
    
    result = bq_client.query(query, job_config=job_config).to_dataframe()
    return result.iloc[0].to_dict() if not result.empty else {}

def get_signals_data(bq_client, start_date, end_date):
    """Get clustered cases as signals"""
    
    # First get clusters from daily_detected_clusters table
    clusters_query = f"""
        SELECT cluster_id, unique_ids
        FROM `{PROJECT_ID}.{DATASET_ID}.daily_detected_clusters`
        WHERE detection_date BETWEEN @start_date AND @end_date
        AND merge_status = 'accepted'
        ORDER BY cluster_id
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date)
        ]
    )
    
    clusters_df = bq_client.query(clusters_query, job_config=job_config).to_dataframe()
    
    if clusters_df.empty:
        return []
    
    signals = []
    
    for _, cluster in clusters_df.iterrows():
        unique_ids = cluster['unique_ids'].split(',') if cluster['unique_ids'] else []
        
        if not unique_ids:
            continue
            
        # Get patient details for this cluster
        patients_query = f"""
            SELECT 
                unique_id,
                patient_name,
                pat_age,
                patient_id,
                pat_date_admission,
                statename,
                districtname,
                subdistrictname,
                villagename,
                sym_fever,
                sym_headache,
                sym_myalgia,
                sym_chills,
                sym_diarrhea,
                sym_cough,
                sym_breathlessness,
                sym_sore_throat,
                sym_altered_sensorium,
                sym_new_onset_of_seizures,
                sym_neck_rigidity,
                sym_jaundice,
                sym_yellow_skin,
                sym_yellow_sclera,
                sym_papule_rash
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            WHERE unique_id IN UNNEST(@unique_ids)
            ORDER BY pat_date_admission
        """
        
        patients_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("unique_ids", "STRING", unique_ids)
            ]
        )
        
        patients_df = bq_client.query(patients_query, job_config=patients_config).to_dataframe()
        
        if not patients_df.empty:
            # Determine syndrome for this cluster
            syndrome = determine_syndrome(patients_df)
            
            signal_data = {
                'signal_id': f"MMCS{cluster['cluster_id']:04d}",
                'syndrome': syndrome,
                'cases': []
            }
            
            for _, patient in patients_df.iterrows():
                case = {
                    'admission_date': patient['pat_date_admission'].strftime('%d-%m-%Y') if patient['pat_date_admission'] else '',
                    'state': patient['statename'] or '',
                    'district': patient['districtname'] or '',
                    'sub_district': patient['subdistrictname'] or '',
                    'village': patient['villagename'] or '',
                    'name': patient['patient_name'] or '',
                    'age': int(patient['pat_age']) if patient['pat_age'] else 0,
                    'patient_id': str(patient['patient_id']) if patient['patient_id'] else ''
                }
                signal_data['cases'].append(case)
            
            signals.append(signal_data)
    
    return signals

def determine_syndrome(patients_df):
    """Determine primary syndrome for a cluster based on symptoms"""
    syndrome_counts = {
        'AUFI': ((patients_df['sym_fever'] == True) & 
                ((patients_df['sym_headache'] == True) | 
                 (patients_df['sym_myalgia'] == True) | 
                 (patients_df['sym_chills'] == True))).sum(),
        'SARI': ((patients_df['sym_fever'] == True) & 
                ((patients_df['sym_cough'] == True) | 
                 (patients_df['sym_breathlessness'] == True) | 
                 (patients_df['sym_sore_throat'] == True))).sum(),
        'ADD': (patients_df['sym_diarrhea'] == True).sum(),
        'AES': ((patients_df['sym_fever'] == True) & 
               ((patients_df['sym_altered_sensorium'] == True) | 
                (patients_df['sym_new_onset_of_seizures'] == True) | 
                (patients_df['sym_neck_rigidity'] == True))).sum()
    }
    
    # Return syndrome with highest count
    return max(syndrome_counts, key=syndrome_counts.get) if any(syndrome_counts.values()) else 'AUFI'