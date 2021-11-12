from google.cloud import bigquery
import os
import argparse
import pandas as pd

# Download query results from bigquery
def download(table_loc):
    # WRITE STATIC COVARIATES (age, gender, ..) INTO STATIC CSV
    query_string = f"""
    SELECT *
    FROM `{table_loc}.icustay_static`
    """

    df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    
    df.to_csv("output/static_variables.csv", index=False)
    print("create static_variables.csv")

    query_string = f"""
    SELECT st.*, ch.sepsis_onset, ch.sepsis_onset_hour
    FROM `{table_loc}.icustay_static` st
    INNER JOIN `{table_loc}.cases` ch
    USING (icustay_id)
    """

    df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    
    df.to_csv("output/static_variables_cases.csv", index=False)
    print("create static_variables_cases.csv")

    query_string = f"""
    SELECT st.*, ch.control_onset_time, ch.control_onset_hour
    FROM `{table_loc}.icustay_static` st
    INNER JOIN `{table_loc}.matched_controls_hourly` ch
    USING (icustay_id)
    """

    df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    
    df.to_csv("output/static_variables_controls.csv", index=False)
    print("create static_variables_controls.csv")

    # Write labs to csv:
    query_string = f"""
    SELECT *
    FROM `{table_loc}.case_48h_hourly_labs`
    ORDER BY icustay_id, chart_time
    """

    df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    
    df.to_csv("output/case_48h_hourly_labs.csv", index=False)
    print("create case_48h_hourly_labs.csv")

    query_string = f"""
    SELECT *
    FROM `{table_loc}.control_48h_hourly_labs`
    ORDER BY icustay_id, chart_time
    """

    df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    
    print("Create control_48h_hourly_labs.csv...")
    df.to_csv("output/control_48h_hourly_labs.csv", index=False)
    
    # Write vitals to csv:
    query_string = f"""
    SELECT *
    FROM `{table_loc}.case_48h_hourly_vitals`
    ORDER BY icustay_id, chart_time
    """

    df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    
    print("Create case_48h_hourly_vitals.csv...")
    df.to_csv("output/case_48h_hourly_vitals.csv", index=False)
    

    query_string = f"""
    SELECT *
    FROM `{table_loc}.control_48h_hourly_vitals`
    ORDER BY icustay_id, chart_time
    """

    df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    
    print("Create control_48h_hourly_vitals.csv...")
    df.to_csv("output/control_48h_hourly_vitals.csv", index=False)

def final_exclusion(window_hr):

    print(f"Excluding icustays with onset_hour <= {window_hr}...")

    static_case = pd.read_csv("output/static_variables_cases.csv")
    print(f"Number of icustays static_variables_cases before exclusion {static_case.shape[0]}")
    # set de-identified ages to median of 91.4
    static_case.loc[static_case.admission_age > 89, 'admission_age'] = 91.4
    # exclude cases with sepsis onset time within window_hr hours of icu admission
    static_case = static_case[static_case.sepsis_onset_hour > window_hr]
    n_case1 = static_case.shape
    print(f"Number of icustays static_variables_cases after exclusion {static_case.shape[0]}")
    static_case.to_csv(f"output/static_variables_cases_ex{window_hr}h.csv")

    static_control = pd.read_csv("output/static_variables_controls.csv")
    print(f"Number of icustays static_variables_controls before exclusion {static_control.shape[0]}")
    # set de-identified ages to median of 91.4
    static_control.loc[static_control.admission_age > 89, 'admission_age'] = 91.4
    # exclude controls with sepsis onset time within window_hr hours of icu admission
    static_control = static_control[static_control.control_onset_hour > window_hr]
    print(f"Number of icustays static_variables_controls after exclusion {static_control.shape[0]}")
    static_control.to_csv(f"output/static_variables_controls_ex{window_hr}h.csv")

    # drop 11 lab measurements had most missing
    DROP_LAB = [
        'ANIONGAP', 'Glucose_CSF', 'Total_Protein_Joint_Fluid',
        'Total_Protein_Pleural', 'Urine_Albumin_Creatinine_ratio',
        'WBC_Ascites', 'Ferritin', 'Transferrin', 'D_Dimer', 
        'SedimentationRate', 'NTproBNP'
    ]

    x = pd.read_csv("output/case_48h_hourly_labs.csv")
    y = x.groupby(['icustay_id', 'chart_time'], as_index=False).mean()
    case_lab_48h = y.drop(columns=DROP_LAB)
    print(f"case_lab_48h before exclusion: # icustays {case_lab_48h.drop_duplicates('icustay_id').shape[0]}, # records {case_lab_48h.shape[0]}")
    case_lab_48h = pd.merge(case_lab_48h, static_case[['icustay_id', 'sepsis_onset']], on='icustay_id', how='inner')
    case_lab_48h['sepsis_onset']= pd.to_datetime(case_lab_48h.sepsis_onset)
    case_lab_48h['chart_time'] = pd.to_datetime(case_lab_48h.chart_time)
    # hours before start of prediction window
    case_lab_48h['hr_feature'] = (case_lab_48h.sepsis_onset-case_lab_48h.chart_time)/pd.Timedelta(hours=1) - 3
    # exclude records in prediction window
    case_lab_48h = case_lab_48h[case_lab_48h.hr_feature >= 0]
    print(f"case_lab_48h after exclusion: # icustays {case_lab_48h.drop_duplicates('icustay_id').shape[0]}, # records {case_lab_48h.shape[0]}")
    case_lab_48h.to_csv(f"output/case_48h_labs_ex{window_hr}h.csv")

    x = pd.read_csv("output/control_48h_hourly_labs.csv")
    y = x.groupby(['icustay_id', 'chart_time'], as_index=False).mean()
    control_lab_48h = y.drop(columns=DROP_LAB)
    print(f"control_lab_48h before exclusion: # icustays {control_lab_48h.drop_duplicates('icustay_id').shape[0]}, # records {control_lab_48h.shape[0]}")
    control_lab_48h = pd.merge(control_lab_48h, static_control[['icustay_id', 'control_onset_time']], on='icustay_id', how='inner')
    control_lab_48h['control_onset_time']= pd.to_datetime(control_lab_48h.control_onset_time)
    control_lab_48h['chart_time'] = pd.to_datetime(control_lab_48h.chart_time)
    # hours before start of prediction window
    control_lab_48h['hr_feature'] = (control_lab_48h.control_onset_time-control_lab_48h.chart_time)/pd.Timedelta(hours=1) - 3
    # exclude records in prediction window
    control_lab_48h = control_lab_48h[control_lab_48h.hr_feature >= 0]
    print(f"control_lab_48h after exclusion: # icustays {control_lab_48h.drop_duplicates('icustay_id').shape[0]}, # records {control_lab_48h.shape[0]}")
    control_lab_48h.to_csv(f"output/control_48h_labs_ex{window_hr}h.csv")

    DROP_VITAL = [
        'VitalCapacity', 'TFC',          
        'TPR', 'Flowrate', 'SVI',
        'CRP', 'SV', 'SVV'    
    ]

    x = pd.read_csv("output/case_48h_hourly_vitals.csv")
    y = x.groupby(['icustay_id', 'chart_time'], as_index=False).mean()
    case_vital_48h = y.drop(columns=DROP_VITAL)
    print(f"case_vital_48h before exclusion: # icustays {case_vital_48h.drop_duplicates('icustay_id').shape[0]}, # records {case_vital_48h.shape[0]}")
    case_vital_48h = pd.merge(case_vital_48h, static_case[['icustay_id', 'sepsis_onset']], on='icustay_id', how='inner')
    case_vital_48h['sepsis_onset']= pd.to_datetime(case_vital_48h.sepsis_onset)
    case_vital_48h['chart_time'] = pd.to_datetime(case_vital_48h.chart_time)
    # hours before start of prediction window
    case_vital_48h['hr_feature'] = (case_vital_48h.sepsis_onset-case_vital_48h.chart_time)/pd.Timedelta(hours=1) - 3
    # exclude records in prediction window
    case_vital_48h = case_vital_48h[case_vital_48h.hr_feature >= 0]
    print(f"case_vital_48h after exclusion: # icustays {case_vital_48h.drop_duplicates('icustay_id').shape[0]}, # records {case_vital_48h.shape[0]}")
    case_vital_48h.to_csv(f"output/case_48h_vitals_ex{window_hr}h.csv")

    x = pd.read_csv("output/control_48h_hourly_vitals.csv")
    y = x.groupby(['icustay_id', 'chart_time'], as_index=False).mean()
    control_vital_48h = y.drop(columns=DROP_VITAL)
    print(f"control_vital_48h before exclusion: # icustays {control_vital_48h.drop_duplicates('icustay_id').shape[0]}, # records {control_vital_48h.shape[0]}")
    control_vital_48h = pd.merge(control_vital_48h, static_control[['icustay_id', 'control_onset_time']], on='icustay_id', how='inner')
    control_vital_48h['control_onset_time']= pd.to_datetime(control_vital_48h.control_onset_time)
    control_vital_48h['chart_time'] = pd.to_datetime(control_vital_48h.chart_time)
    # hours before start of prediction window
    control_vital_48h['hr_feature'] = (control_vital_48h.control_onset_time-control_vital_48h.chart_time)/pd.Timedelta(hours=1) - 3
    # exclude records in prediction window
    control_vital_48h = control_vital_48h[control_vital_48h.hr_feature >= 0]
    print(f"control_vital_48h after exclusion: # icustays {control_vital_48h.drop_duplicates('icustay_id').shape[0]}, # records {control_vital_48h.shape[0]}")
    control_vital_48h.to_csv(f"output/control_48h_vitals_ex{window_hr}h.csv")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Post-process the exported output files from the SQL query <ENTER_NAME>")
    parser.add_argument("-c", "--credential",
                        help="Google credential json file name under current working directory")
    parser.add_argument("-t", "--tableloc",
                        help="Project and dataset name, e.g. cdc.project")
    parser.add_argument("-w", "--window", default=3, 
                        help="Prediction window length (hour) before onset time, default is 3")
    args = parser.parse_args()
    
    if args.credential:
        # GCP credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), args.credential)
        bqclient = bigquery.Client()
        download(args.tableloc)
    
    final_exclusion(args.window)