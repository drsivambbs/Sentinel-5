import functions_framework
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import io

@functions_framework.http
def export_data(request):
    """Export patient data for past 7 days from given date"""
    
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST',
            'Access-Control-Allow-Headers': 'Content-Type'
        }
        return ('', 204, headers)
    
    try:
        # Get input date
        if request.method == 'GET':
            input_date = request.args.get('date')
        else:
            request_json = request.get_json(silent=True)
            input_date = request_json.get('date') if request_json else None
        
        if not input_date:
            return {'error': 'Date parameter required (YYYY-MM-DD format)'}, 400
        
        # Calculate date range
        end_date = datetime.strptime(input_date, '%Y-%m-%d')
        start_date = end_date - timedelta(days=7)
        
        # Syndrome mapping
        syndrome_map = {
            'Acute Diarrheal Disease': 'ADD',
            'Jaundice of < 4 weeks with fever and/or other abdominal symptoms': 'Jaundice',
            'Acute Undifferentiated Febrile Illness (AUFI) for > 7 days': 'AUFI',
            'Acute Encephalitic Syndrome (AES)': 'AES',
            'Fever with Rash for 7 days': 'Fever with Rash',
            'Acute Flaccid Paralysis': 'AFP',
            'Others': 'Others',
            'Hemorrhagic fever': 'Hemorrhagic fever'
        }
        
        client = bigquery.Client(project='sentinel-h-5')
        
        # Main export - ALL patients from clusters active in date range
        query = f"""
        WITH active_clusters AS (
            SELECT DISTINCT smart_cluster_id
            FROM `sentinel-h-5.sentinel_h_5.smart_clusters`
            WHERE input_date >= '{start_date.strftime('%Y-%m-%d')}'
            AND input_date <= '{end_date.strftime('%Y-%m-%d')}'
        )
        SELECT 
            p.site_code,
            a.smart_cluster_id as cluster_id,
            p.patient_id,
            p.patient_entry_date,
            p.pat_admi_location,
            p.clini_primary_syn,
            p.patient_name,
            p.pat_age,
            p.pat_sex,
            p.statename,
            p.districtname,
            p.subdistrictname,
            p.villagename
        FROM `sentinel-h-5.sentinel_h_5.smart_cluster_assignments` a
        JOIN `sentinel-h-5.sentinel_h_5.patient_records` p ON a.unique_id = p.unique_id
        JOIN active_clusters ac ON a.smart_cluster_id = ac.smart_cluster_id
        ORDER BY a.smart_cluster_id, p.patient_entry_date DESC
        """
        
        df = client.query(query).to_dataframe()
        
        if df.empty:
            return {'message': 'No clustered data found for the specified date range'}, 404
        
        # Map syndromes, admission locations, and sex
        df['Syndrome'] = df['clini_primary_syn'].map(syndrome_map).fillna(df['clini_primary_syn'])
        
        # Map admission locations
        location_map = {
            1: 'General Ward',
            2: 'Pediatrics Ward', 
            3: 'ICU'
        }
        df['Ward'] = df['pat_admi_location'].map(location_map).fillna(df['pat_admi_location'])
        
        # Map sex
        sex_map = {1: 'M', 2: 'F'}
        df['Sex'] = df['pat_sex'].map(sex_map).fillna(df['pat_sex'])
        
        # Select and rename final columns in requested order
        export_df = df[[
            'site_code', 'cluster_id', 'patient_id', 'patient_entry_date', 'Ward',
            'Syndrome', 'patient_name', 'pat_age', 'Sex', 'statename', 
            'districtname', 'subdistrictname', 'villagename'
        ]].rename(columns={
            'patient_entry_date': 'Date of Admission',
            'patient_name': 'Patient Name',
            'pat_age': 'Age (in years)',
            'patient_id': 'Patient ID',
            'statename': 'State',
            'districtname': 'District', 
            'subdistrictname': 'Sub district',
            'villagename': 'Village'
        })
        
        # Query for newly added cases (cases added AFTER cluster creation date)
        expansion_query = f"""
        WITH active_clusters AS (
            SELECT smart_cluster_id, original_creation_date
            FROM `sentinel-h-5.sentinel_h_5.smart_clusters`
            WHERE input_date >= '{start_date.strftime('%Y-%m-%d')}'
            AND input_date <= '{end_date.strftime('%Y-%m-%d')}'
        )
        SELECT 
            p.site_code,
            a.smart_cluster_id,
            p.patient_id,
            p.patient_entry_date,
            p.pat_admi_location,
            p.clini_primary_syn,
            p.patient_name,
            p.pat_age,
            p.pat_sex,
            p.statename,
            p.districtname,
            p.subdistrictname,
            p.villagename
        FROM `sentinel-h-5.sentinel_h_5.smart_cluster_assignments` a
        JOIN `sentinel-h-5.sentinel_h_5.patient_records` p ON a.unique_id = p.unique_id
        JOIN active_clusters ac ON a.smart_cluster_id = ac.smart_cluster_id
        WHERE a.addition_type = 'EXPANSION'
        AND a.expansion_date >= '{start_date.strftime('%Y-%m-%d')}'
        AND a.expansion_date <= '{end_date.strftime('%Y-%m-%d')}'
        ORDER BY a.smart_cluster_id, a.expansion_date DESC
        """
        
        expansion_df = client.query(expansion_query).to_dataframe()
        
        # Process expansion columns same as main sheet
        if not expansion_df.empty:
            # Map syndromes, admission locations, and sex for expansion cases
            expansion_df['Syndrome'] = expansion_df['clini_primary_syn'].map(syndrome_map).fillna(expansion_df['clini_primary_syn'])
            
            location_map = {
                1: 'General Ward',
                2: 'Pediatrics Ward', 
                3: 'ICU'
            }
            expansion_df['Ward'] = expansion_df['pat_admi_location'].map(location_map).fillna(expansion_df['pat_admi_location'])
            
            sex_map = {1: 'M', 2: 'F'}
            expansion_df['Sex'] = expansion_df['pat_sex'].map(sex_map).fillna(expansion_df['pat_sex'])
            
            # Select and rename columns in same order as main sheet
            expansion_df = expansion_df[[
                'site_code', 'smart_cluster_id', 'patient_id', 'patient_entry_date', 'Ward',
                'Syndrome', 'patient_name', 'pat_age', 'Sex', 'statename', 
                'districtname', 'subdistrictname', 'villagename'
            ]].rename(columns={
                'smart_cluster_id': 'cluster_id',
                'patient_entry_date': 'Date of Admission',
                'patient_name': 'Patient Name',
                'pat_age': 'Age (in years)',
                'patient_id': 'Patient ID',
                'statename': 'State',
                'districtname': 'District', 
                'subdistrictname': 'Sub district',
                'villagename': 'Village'
            })
        
        # Create summary info
        summary_data = {
            'Parameter': ['Input Date', 'Processing Date Frame'],
            'Value': [input_date, f'{start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}']
        }
        summary_df = pd.DataFrame(summary_data)
        
        # Create Excel file with summary and data sheets
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Summary sheet
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Data sheets
            export_df.to_excel(writer, sheet_name='All_Cluster_Cases', index=False)
            if not expansion_df.empty:
                expansion_df.to_excel(writer, sheet_name='Newly_Added_Cases', index=False)
        
        output.seek(0)
        
        headers = {
            'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'Content-Disposition': f'attachment; filename=cluster_report_{input_date}.xlsx',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST',
            'Access-Control-Allow-Headers': 'Content-Type'
        }
        
        return output.getvalue(), 200, headers
        
    except ValueError as e:
        return {'error': f'Invalid date format: {str(e)}'}, 400
    except Exception as e:
        return {'error': f'Export failed: {str(e)}'}, 500