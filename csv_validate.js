const API_ENDPOINT = 'https://sentinel-upload-service-196547645490.asia-south1.run.app';

const referenceSchema = [{"column_name":"patient_id","data_type":"INT64"},{"column_name":"site_code","data_type":"STRING"},{"column_name":"hospital_reg_id","data_type":"STRING"},{"column_name":"patient_entry_date","data_type":"DATE"},{"column_name":"pat_admi_location","data_type":"INT64"},{"column_name":"patient_name","data_type":"STRING"},{"column_name":"pat_mid_name","data_type":"STRING"},{"column_name":"pat_sodo","data_type":"STRING"},{"column_name":"pat_age","data_type":"INT64"},{"column_name":"pat_agemonths","data_type":"STRING"},{"column_name":"pat_agedays","data_type":"INT64"},{"column_name":"pat_sex","data_type":"INT64"},{"column_name":"pat_contact_his","data_type":"INT64"},{"column_name":"pat_health_worker","data_type":"INT64"},{"column_name":"pat_hw_contact","data_type":"INT64"},{"column_name":"pat_travel_his","data_type":"INT64"},{"column_name":"pat_travel_type","data_type":"INT64"},{"column_name":"pat_travel_int","data_type":"STRING"},{"column_name":"pat_travel_dom","data_type":"INT64"},{"column_name":"pat_country","data_type":"STRING"},{"column_name":"statename","data_type":"STRING"},{"column_name":"districtname","data_type":"STRING"},{"column_name":"subdistrictname","data_type":"STRING"},{"column_name":"villagename","data_type":"STRING"},{"column_name":"latitude","data_type":"FLOAT64"},{"column_name":"longitude","data_type":"FLOAT64"},{"column_name":"pat_street","data_type":"STRING"},{"column_name":"pat_house","data_type":"STRING"},{"column_name":"pat_pincode","data_type":"INT64"},{"column_name":"pat_areatype","data_type":"STRING"},{"column_name":"pat_occupation","data_type":"INT64"},{"column_name":"pat_occupation_others","data_type":"STRING"},{"column_name":"clini_date_illness","data_type":"STRING"},{"column_name":"clini_dura_days","data_type":"INT64"},{"column_name":"clini_primary_syn","data_type":"STRING"},{"column_name":"syndrome_other_p","data_type":"STRING"},{"column_name":"sym_fever","data_type":"BOOL"},{"column_name":"sym_new_onset_of_seizures","data_type":"BOOL"},{"column_name":"sym_dysentery","data_type":"BOOL"},{"column_name":"sym_jaundice","data_type":"BOOL"},{"column_name":"sym_papule_rash","data_type":"BOOL"},{"column_name":"sym_vomiting","data_type":"BOOL"},{"column_name":"sym_myalgia","data_type":"BOOL"},{"column_name":"sym_breathlessness","data_type":"BOOL"},{"column_name":"sym_sore_throat","data_type":"BOOL"},{"column_name":"sym_cough","data_type":"BOOL"},{"column_name":"sym_yellow_urine","data_type":"BOOL"},{"column_name":"sym_yellow_skin","data_type":"BOOL"},{"column_name":"sym_eschar","data_type":"STRING"},{"column_name":"sym_altered_sensorium","data_type":"BOOL"},{"column_name":"sym_increased_somnolence","data_type":"BOOL"},{"column_name":"sym_dark_urine","data_type":"BOOL"},{"column_name":"sym_yellow_sclera","data_type":"BOOL"},{"column_name":"sym_nausea","data_type":"BOOL"},{"column_name":"sym_irritability","data_type":"BOOL"},{"column_name":"sym_diarrhea","data_type":"BOOL"},{"column_name":"sym_neck_rigidity","data_type":"BOOL"},{"column_name":"sym_headache","data_type":"BOOL"},{"column_name":"sym_chills","data_type":"BOOL"},{"column_name":"sym_malaise","data_type":"BOOL"},{"column_name":"sym_discharge_eyes","data_type":"STRING"},{"column_name":"sym_arthralgia","data_type":"BOOL"},{"column_name":"sym_abdominal_pain","data_type":"BOOL"},{"column_name":"sym_redness_eyes","data_type":"BOOL"},{"column_name":"sym_retro_orb_pain","data_type":"BOOL"},{"column_name":"sym_other_symptom","data_type":"BOOL"},{"column_name":"symptom_other","data_type":"STRING"},{"column_name":"vit_pulse","data_type":"INT64"},{"column_name":"vit_bp_sys","data_type":"INT64"},{"column_name":"vit_bp_dia","data_type":"INT64"},{"column_name":"vit_temp","data_type":"INT64"},{"column_name":"vit_resp_rate","data_type":"INT64"},{"column_name":"vit_spo2","data_type":"INT64"},{"column_name":"vit_dehyd","data_type":"INT64"},{"column_name":"vit_icterus","data_type":"INT64"},{"column_name":"vit_edema","data_type":"INT64"},{"column_name":"vit_conj","data_type":"INT64"},{"column_name":"vit_conj_sub","data_type":"INT64"},{"column_name":"vit_eschar","data_type":"INT64"},{"column_name":"vit_eschar_value","data_type":"INT64"},{"column_name":"vit_cbc_done","data_type":"INT64"},{"column_name":"vit_cbc_hemat","data_type":"FLOAT64"},{"column_name":"vit_cbc_hb","data_type":"FLOAT64"},{"column_name":"vit_cbc_rbc","data_type":"FLOAT64"},{"column_name":"vit_cbc_wbc","data_type":"FLOAT64"},{"column_name":"vit_cbc_dc_n","data_type":"FLOAT64"},{"column_name":"vit_cbc_dc_l","data_type":"FLOAT64"},{"column_name":"vit_cbc_dc_m","data_type":"FLOAT64"},{"column_name":"vit_cbc_dc_e","data_type":"FLOAT64"},{"column_name":"vit_cbc_dc_b","data_type":"FLOAT64"},{"column_name":"vit_cbc_esr","data_type":"FLOAT64"},{"column_name":"vit_cbc_plat","data_type":"INT64"},{"column_name":"vit_lf_done","data_type":"INT64"},{"column_name":"vit_lf_al","data_type":"FLOAT64"},{"column_name":"vit_lf_pro","data_type":"FLOAT64"},{"column_name":"vit_lf_ast","data_type":"FLOAT64"},{"column_name":"vit_lf_alt","data_type":"FLOAT64"},{"column_name":"vit_lf_ggt","data_type":"INT64"},{"column_name":"vit_lf_bil_d","data_type":"FLOAT64"},{"column_name":"vit_lf_bil_i","data_type":"FLOAT64"},{"column_name":"vit_lf_bil_t","data_type":"FLOAT64"},{"column_name":"vit_rf_done","data_type":"INT64"},{"column_name":"vit_rf_urea","data_type":"FLOAT64"},{"column_name":"vit_rf_creat","data_type":"FLOAT64"},{"column_name":"vit_rf_chl","data_type":"FLOAT64"},{"column_name":"vit_rf_sod","data_type":"FLOAT64"},{"column_name":"vit_rf_pot","data_type":"FLOAT64"},{"column_name":"vit_rf_uric","data_type":"FLOAT64"},{"column_name":"vit_rf_cal","data_type":"FLOAT64"},{"column_name":"vit_rf_urea_r","data_type":"INT64"},{"column_name":"vit_apr_done","data_type":"INT64"},{"column_name":"vit_apr_crp","data_type":"FLOAT64"},{"column_name":"vit_apr_ldh","data_type":"FLOAT64"},{"column_name":"vit_apr_ddimer","data_type":"FLOAT64"},{"column_name":"vit_apr_ferr","data_type":"FLOAT64"},{"column_name":"vit_apr_tferr","data_type":"FLOAT64"},{"column_name":"vit_apr_proca","data_type":"FLOAT64"},{"column_name":"vit_diag","data_type":"STRING"},{"column_name":"vit_comorbid","data_type":"INT64"},{"column_name":"vit_comorbid_others","data_type":"STRING"},{"column_name":"vit_icu_shift","data_type":"INT64"},{"column_name":"vit_icu_duration","data_type":"INT64"},{"column_name":"vit_outcome","data_type":"INT64"},{"column_name":"vit_death_cause","data_type":"STRING"},{"column_name":"vit_discharge_date","data_type":"STRING"}];

function validateCSV() {
    const file = document.getElementById('csvFile').files[0];
    if (!file) return alert('Select a CSV file');
    
    const reader = new FileReader();
    reader.onload = function(e) {
        const csvData = Papa.parse(e.target.result, { header: true, skipEmptyLines: true });
        const headers = csvData.meta.fields;
        const rows = csvData.data;
        const requiredColumns = referenceSchema.map(col => col.column_name);
        
        let errors = [];
        
        headers.forEach(header => {
            if (!requiredColumns.includes(header)) {
                errors.push(`Unknown column: ${header}`);
            }
        });
        
        requiredColumns.forEach(required => {
            if (!headers.includes(required)) {
                errors.push(`Missing column: ${required}`);
            }
        });
        
        if (errors.length) {
            document.getElementById('result').innerHTML = `<h3 style="color:red">Errors:</h3><ul><li>${errors.join('</li><li>')}</li></ul>`;
        } else {
            const processedRows = rows.map(row => {
                row.latitude = '';
                row.longitude = '';
                
                const addressParts = [
                    row.pat_house || '',
                    row.pat_street || '',
                    row.villagename || '',
                    row.subdistrictname || '',
                    row.districtname || '',
                    row.statename || '',
                    row.pat_pincode || ''
                ].filter(part => part.trim()).join(' | ');
                
                row.complete_address_column = addressParts;
                row.unique_id = `${row.patient_id || ''}_${row.site_code || ''}_${row.patient_entry_date || ''}`;
                
                return row;
            });
            
            const modifiedCSV = Papa.unparse(processedRows);
            const blob = new Blob([modifiedCSV], { type: 'text/plain' });
            const url = URL.createObjectURL(blob);
            
            const formData = new FormData();
            formData.append('file', new File([modifiedCSV], `validated_${file.name}`, { type: 'text/csv' }));
            
            fetch(`${API_ENDPOINT}/upload`, {
                method: 'POST',
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                document.getElementById('result').innerHTML = `
                    <h3 style="color:green">✓ All ${requiredColumns.length} columns present!</h3>
                    <p style="color:green">File uploaded to GCS: ${data.fileName}</p>
                    <a href="${url}" download="validated_${file.name}" style="background:blue;color:white;padding:10px;text-decoration:none;">Download Cleaned File</a>
                `;
            })
            .catch(error => {
                document.getElementById('result').innerHTML = `
                    <h3 style="color:green">✓ All ${requiredColumns.length} columns present!</h3>
                    <p style="color:red">Upload failed: ${error.message}</p>
                    <a href="${url}" download="validated_${file.name}" style="background:blue;color:white;padding:10px;text-decoration:none;">Download Cleaned File</a>
                `;
            });
        }
    };
    reader.readAsText(file);
}