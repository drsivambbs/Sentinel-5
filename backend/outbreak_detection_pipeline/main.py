import os
import logging
from datetime import datetime, timedelta
from google.cloud import bigquery
from flask import Flask, request, jsonify

app = Flask(__name__)

PROJECT_ID = os.getenv("PROJECT_ID", "sentinel-h-5")
DATASET_ID = os.getenv("DATASET_ID", "sentinel_h_5")
TABLE = f"{PROJECT_ID}.{DATASET_ID}.patient_records"
CLUSTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.daily_detected_clusters"

client = bigquery.Client()
logging.basicConfig(level=logging.INFO)

@app.route('/outbreak-detection-step1', methods=['POST', 'GET'])
def detect_clusters():
    try:
        request_json = request.get_json(silent=True) or {}
        process_date = None

        # === 1. Determine date to process ===
        try:
            client.get_table(CLUSTER_TABLE)
            table_exists = True
        except:
            table_exists = False

        if not table_exists:
            row = client.query(f"SELECT MAX(patient_entry_date) AS d FROM `{TABLE}`").result()
            max_date = next(row).d
            if not max_date:
                return jsonify({"status": "error", "message": "No data"}), 400
            process_date = max_date - timedelta(days=7)
        else:
            row = client.query(f"SELECT MAX(input_date) AS d FROM `{CLUSTER_TABLE}`").result()
            last = next(row).d
            process_date = (last + timedelta(days=1)) if last else None
            if not process_date:
                row = client.query(f"SELECT MAX(patient_entry_date) AS d FROM `{TABLE}`").result()
                max_date = next(row).d
                process_date = max_date - timedelta(days=7) if max_date else None

        if request_json.get("date"):
            process_date = datetime.strptime(request_json["date"], "%Y-%m-%d").date()

        if not process_date:
            return jsonify({"status": "error", "message": "No date"}), 400

        logging.info(f"Processing date: {process_date}")

        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("dt", "DATE", process_date)]
        )

        # === 2. Ensure input_date column exists ===
        if table_exists:
            client.query(f"""
                ALTER TABLE `{CLUSTER_TABLE}`
                ADD COLUMN IF NOT EXISTS input_date DATE
            """).result()

        # === 3. Skip forward until ≥90% geocoded ===
        current = process_date
        job_config.query_parameters[0].value = current
        for _ in range(15):
            row = next(client.query(f"""
                SELECT 
                  COUNT(*) AS total,
                  COUNTIF(latitude IS NOT NULL AND longitude IS NOT NULL) AS geo
                FROM `{TABLE}` WHERE DATE(patient_entry_date) = @dt
            """, job_config=job_config).result())
            if row.total > 0 and (row.geo / row.total) >= 0.9:
                process_date = current
                break
            current += timedelta(days=1)
            job_config.query_parameters[0].value = current
        else:
            return jsonify({"status": "error", "message": "No good geocoding in 15 days"}), 400

        job_config.query_parameters[0].value = process_date

        # === 4. CREATE TABLE WITH EXACT 133 COLUMNS ===
        if not table_exists:
            client.query(f"""
                CREATE TABLE `{CLUSTER_TABLE}` (
                  daily_cluster_id          STRING,
                  cluster_accepted_status   STRING,
                  algorithm_type            STRING,
                  actual_cluster_radius     FLOAT64,
                  dummy_id                  STRING,
                  input_date                DATE,
                  patient_id                INT64,
                  site_code                 STRING,
                  hospital_reg_id           STRING,
                  patient_entry_date        DATE,
                  pat_admi_location         INT64,
                  patient_name              STRING,
                  pat_mid_name              STRING,
                  pat_sodo                  STRING,
                  pat_age                   INT64,
                  pat_agemonths             STRING,
                  pat_agedays               INT64,
                  pat_sex                   INT64,
                  pat_contact_his           INT64,
                  pat_health_worker         INT64,
                  pat_hw_contact            INT64,
                  pat_travel_his            INT64,
                  pat_travel_type           INT64,
                  pat_travel_int            STRING,
                  pat_travel_dom            INT64,
                  pat_country               STRING,
                  statename                 STRING,
                  districtname              STRING,
                  subdistrictname           STRING,
                  villagename               STRING,
                  latitude                  FLOAT64,
                  longitude                 FLOAT64,
                  pat_street                STRING,
                  pat_house                 STRING,
                  pat_pincode               INT64,
                  pat_areatype              STRING,
                  pat_occupation            INT64,
                  pat_occupation_others     STRING,
                  clini_date_illness        STRING,
                  clini_dura_days           INT64,
                  clini_primary_syn         STRING,
                  syndrome_other_p          STRING,
                  sym_fever                 BOOL,
                  sym_new_onset_of_seizures BOOL,
                  sym_dysentery             BOOL,
                  sym_jaundice              BOOL,
                  sym_papule_rash           BOOL,
                  sym_vomiting              BOOL,
                  sym_myalgia               BOOL,
                  sym_breathlessness        BOOL,
                  sym_sore_throat           BOOL,
                  sym_cough                 BOOL,
                  sym_yellow_urine          BOOL,
                  sym_yellow_skin           BOOL,
                  sym_eschar                STRING,
                  sym_altered_sensorium     BOOL,
                  sym_increased_somnolence  BOOL,
                  sym_dark_urine            BOOL,
                  sym_yellow_sclera         BOOL,
                  sym_nausea                BOOL,
                  sym_irritability          BOOL,
                  sym_diarrhea              BOOL,
                  sym_neck_rigidity         BOOL,
                  sym_headache              BOOL,
                  sym_chills                BOOL,
                  sym_malaise               BOOL,
                  sym_discharge_eyes        STRING,
                  sym_arthralgia            BOOL,
                  sym_abdominal_pain        BOOL,
                  sym_redness_eyes          BOOL,
                  sym_retro_orb_pain        BOOL,
                  sym_other_symptom         BOOL,
                  symptom_other             STRING,
                  vit_pulse                 INT64,
                  vit_bp_sys                INT64,
                  vit_bp_dia                INT64,
                  vit_temp                  INT64,
                  vit_resp_rate             INT64,
                  vit_spo2                  INT64,
                  vit_dehyd                 INT64,
                  vit_icterus               INT64,
                  vit_edema                 INT64,
                  vit_conj                  INT64,
                  vit_conj_sub              INT64,
                  vit_eschar                INT64,
                  vit_eschar_value          INT64,
                  vit_cbc_done              INT64,
                  vit_cbc_hemat             FLOAT64,
                  vit_cbc_hb                FLOAT64,
                  vit_cbc_rbc               FLOAT64,
                  vit_cbc_wbc               FLOAT64,
                  vit_cbc_dc_n              FLOAT64,
                  vit_cbc_dc_l              FLOAT64,
                  vit_cbc_dc_m              FLOAT64,
                  vit_cbc_dc_e              FLOAT64,
                  vit_cbc_dc_b              FLOAT64,
                  vit_cbc_esr               FLOAT64,
                  vit_cbc_plat              INT64,
                  vit_lf_done               INT64,
                  vit_lf_al                 FLOAT64,
                  vit_lf_pro                FLOAT64,
                  vit_lf_ast                FLOAT64,
                  vit_lf_alt                FLOAT64,
                  vit_lf_ggt                INT64,
                  vit_lf_bil_d              FLOAT64,
                  vit_lf_bil_i              FLOAT64,
                  vit_lf_bil_t              FLOAT64,
                  vit_rf_done               INT64,
                  vit_rf_urea               FLOAT64,
                  vit_rf_creat              FLOAT64,
                  vit_rf_chl                FLOAT64,
                  vit_rf_sod                FLOAT64,
                  vit_rf_pot                FLOAT64,
                  vit_rf_uric               FLOAT64,
                  vit_rf_cal                FLOAT64,
                  vit_rf_urea_r             INT64,
                  vit_apr_done              INT64,
                  vit_apr_crp               FLOAT64,
                  vit_apr_ldh               FLOAT64,
                  vit_apr_ddimer            FLOAT64,
                  vit_apr_ferr              FLOAT64,
                  vit_apr_tferr             FLOAT64,
                  vit_apr_proca             FLOAT64,
                  vit_diag                  STRING,
                  vit_comorbid              INT64,
                  vit_comorbid_others       STRING,
                  vit_icu_shift             INT64,
                  vit_icu_duration          INT64,
                  vit_outcome               INT64,
                  vit_death_cause           STRING,
                  vit_discharge_date        STRING,
                  complete_address_column   STRING,
                  unique_id                 STRING
                )
                PARTITION BY patient_entry_date
                CLUSTER BY algorithm_type, cluster_accepted_status
            """).result()
            logging.info("Table created with exact schema")

        # === 5. ABC Algorithm ===
        abc_sql = f"""
        INSERT INTO `{CLUSTER_TABLE}` (
          daily_cluster_id, cluster_accepted_status, algorithm_type, actual_cluster_radius, dummy_id, input_date,
          patient_id, site_code, hospital_reg_id, patient_entry_date, pat_admi_location, patient_name, pat_mid_name,
          pat_sodo, pat_age, pat_agemonths, pat_agedays, pat_sex, pat_contact_his, pat_health_worker,
          pat_hw_contact, pat_travel_his, pat_travel_type, pat_travel_int, pat_travel_dom, pat_country,
          statename, districtname, subdistrictname, villagename, latitude, longitude,
          pat_street, pat_house, pat_pincode, pat_areatype, pat_occupation, pat_occupation_others,
          clini_date_illness, clini_dura_days, clini_primary_syn, syndrome_other_p,
          sym_fever, sym_new_onset_of_seizures, sym_dysentery, sym_jaundice, sym_papule_rash,
          sym_vomiting, sym_myalgia, sym_breathlessness, sym_sore_throat, sym_cough,
          sym_yellow_urine, sym_yellow_skin, sym_eschar, sym_altered_sensorium, sym_increased_somnolence,
          sym_dark_urine, sym_yellow_sclera, sym_nausea, sym_irritability, sym_diarrhea,
          sym_neck_rigidity, sym_headache, sym_chills, sym_malaise, sym_discharge_eyes,
          sym_arthralgia, sym_abdominal_pain, sym_redness_eyes, sym_retro_orb_pain,
          sym_other_symptom, symptom_other, vit_pulse, vit_bp_sys, vit_bp_dia, vit_temp,
          vit_resp_rate, vit_spo2, vit_dehyd, vit_icterus, vit_edema, vit_conj,
          vit_conj_sub, vit_eschar, vit_eschar_value, vit_cbc_done, vit_cbc_hemat,
          vit_cbc_hb, vit_cbc_rbc, vit_cbc_wbc, vit_cbc_dc_n, vit_cbc_dc_l,
          vit_cbc_dc_m, vit_cbc_dc_e, vit_cbc_dc_b, vit_cbc_esr, vit_cbc_plat,
          vit_lf_done, vit_lf_al, vit_lf_pro, vit_lf_ast, vit_lf_alt, vit_lf_ggt,
          vit_lf_bil_d, vit_lf_bil_i, vit_lf_bil_t, vit_rf_done, vit_rf_urea,
          vit_rf_creat, vit_rf_chl, vit_rf_sod, vit_rf_pot, vit_rf_uric,
          vit_rf_cal, vit_rf_urea_r, vit_apr_done, vit_apr_crp, vit_apr_ldh,
          vit_apr_ddimer, vit_apr_ferr, vit_apr_tferr, vit_apr_proca,
          vit_diag, vit_comorbid, vit_comorbid_others, vit_icu_shift, vit_icu_duration,
          vit_outcome, vit_death_cause, vit_discharge_date, complete_address_column, unique_id
        )
        WITH src AS (
          SELECT *, ST_GEOGPOINT(longitude, latitude) AS geo_pt
          FROM `{TABLE}`
          WHERE DATE(patient_entry_date) BETWEEN DATE_SUB(@dt, INTERVAL 6 DAY) AND @dt
            AND latitude IS NOT NULL AND longitude IS NOT NULL
            AND pat_areatype = 'Rural'
            AND clini_primary_syn IS NOT NULL
            AND villagename IS NOT NULL
        ),
        grouped AS (
          SELECT *, COUNT(*) OVER (PARTITION BY villagename, clini_primary_syn) AS case_count
          FROM src
        )
        SELECT
          CONCAT('ABC_', villagename, '_', clini_primary_syn, '_', FORMAT_DATE('%m%d%Y', @dt)),
          'pending', 'ABC', 100.0,
          CONCAT('ABC_', villagename, '_', clini_primary_syn, '_', unique_id),
          @dt,
          g.* EXCEPT(geo_pt, case_count)
        FROM grouped g
        WHERE case_count >= 2
        """
        client.query(abc_sql, job_config=job_config).result()

        # === 6. GIS Algorithm ===
        gis_sql = f"""
        INSERT INTO `{CLUSTER_TABLE}` (
          daily_cluster_id, cluster_accepted_status, algorithm_type, actual_cluster_radius, dummy_id, input_date,
          patient_id, site_code, hospital_reg_id, patient_entry_date, pat_admi_location, patient_name, pat_mid_name,
          pat_sodo, pat_age, pat_agemonths, pat_agedays, pat_sex, pat_contact_his, pat_health_worker,
          pat_hw_contact, pat_travel_his, pat_travel_type, pat_travel_int, pat_travel_dom, pat_country,
          statename, districtname, subdistrictname, villagename, latitude, longitude,
          pat_street, pat_house, pat_pincode, pat_areatype, pat_occupation, pat_occupation_others,
          clini_date_illness, clini_dura_days, clini_primary_syn, syndrome_other_p,
          sym_fever, sym_new_onset_of_seizures, sym_dysentery, sym_jaundice, sym_papule_rash,
          sym_vomiting, sym_myalgia, sym_breathlessness, sym_sore_throat, sym_cough,
          sym_yellow_urine, sym_yellow_skin, sym_eschar, sym_altered_sensorium, sym_increased_somnolence,
          sym_dark_urine, sym_yellow_sclera, sym_nausea, sym_irritability, sym_diarrhea,
          sym_neck_rigidity, sym_headache, sym_chills, sym_malaise, sym_discharge_eyes,
          sym_arthralgia, sym_abdominal_pain, sym_redness_eyes, sym_retro_orb_pain,
          sym_other_symptom, symptom_other, vit_pulse, vit_bp_sys, vit_bp_dia, vit_temp,
          vit_resp_rate, vit_spo2, vit_dehyd, vit_icterus, vit_edema, vit_conj,
          vit_conj_sub, vit_eschar, vit_eschar_value, vit_cbc_done, vit_cbc_hemat,
          vit_cbc_hb, vit_cbc_rbc, vit_cbc_wbc, vit_cbc_dc_n, vit_cbc_dc_l,
          vit_cbc_dc_m, vit_cbc_dc_e, vit_cbc_dc_b, vit_cbc_esr, vit_cbc_plat,
          vit_lf_done, vit_lf_al, vit_lf_pro, vit_lf_ast, vit_lf_alt, vit_lf_ggt,
          vit_lf_bil_d, vit_lf_bil_i, vit_lf_bil_t, vit_rf_done, vit_rf_urea,
          vit_rf_creat, vit_rf_chl, vit_rf_sod, vit_rf_pot, vit_rf_uric,
          vit_rf_cal, vit_rf_urea_r, vit_apr_done, vit_apr_crp, vit_apr_ldh,
          vit_apr_ddimer, vit_apr_ferr, vit_apr_tferr, vit_apr_proca,
          vit_diag, vit_comorbid, vit_comorbid_others, vit_icu_shift, vit_icu_duration,
          vit_outcome, vit_death_cause, vit_discharge_date, complete_address_column, unique_id
        )
        WITH src AS (
          SELECT *, ST_GEOGPOINT(longitude, latitude) AS geo_pt
          FROM `{TABLE}`
          WHERE DATE(patient_entry_date) BETWEEN DATE_SUB(@dt, INTERVAL 6 DAY) AND @dt
            AND latitude IS NOT NULL AND longitude IS NOT NULL
            AND pat_areatype = 'Urban'
            AND clini_primary_syn IS NOT NULL
        ),
        clustered AS (
          SELECT *,
            ST_CLUSTERDBSCAN(geo_pt, 350, 2) OVER (PARTITION BY clini_primary_syn) AS cluster_id
          FROM src
        ),
        centroids AS (
          SELECT clini_primary_syn, cluster_id, ST_CENTROID_AGG(geo_pt) AS centroid
          FROM clustered WHERE cluster_id IS NOT NULL
          GROUP BY clini_primary_syn, cluster_id
        ),
        with_dist AS (
          SELECT c.*, cent.centroid, ST_DISTANCE(c.geo_pt, cent.centroid) AS dist_m
          FROM clustered c
          JOIN centroids cent USING (clini_primary_syn, cluster_id)
        ),
        radius AS (
          SELECT clini_primary_syn, cluster_id, MAX(dist_m) AS max_radius
          FROM with_dist
          GROUP BY clini_primary_syn, cluster_id
        )
        SELECT
          CONCAT('GIS_', wd.clini_primary_syn, '_', wd.cluster_id, '_', FORMAT_DATE('%m%d%Y', @dt)),
          'pending', 'GIS', COALESCE(r.max_radius, 0),
          CONCAT('GIS_', wd.clini_primary_syn, '_', wd.cluster_id, '_', wd.unique_id),
          @dt,
          wd.* EXCEPT(geo_pt, centroid, dist_m, cluster_id)
        FROM with_dist wd
        JOIN radius r USING (clini_primary_syn, cluster_id)
        """
        client.query(gis_sql, job_config=job_config).result()

        # === 7. Count clusters ===
        count_result = client.query(f"""
            SELECT COUNT(DISTINCT daily_cluster_id) AS n
            FROM `{CLUSTER_TABLE}`
            WHERE input_date = @dt
        """, job_config=job_config).result()
        cluster_count = next(count_result).n

        return jsonify({"status": "success", "input_date": str(process_date), "clusters": cluster_count})

    except Exception as e:
        logging.exception("Failed")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

