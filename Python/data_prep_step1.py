from google.cloud import bigquery
import os
import argparse
import pandas as pd

# Download query results from bigquery
def download(table_loc, series_len):
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
    print("Create static_variables.csv")

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
    print("Create static_variables_cases.csv")

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
    print("Create static_variables_controls.csv")

    # Write labs to csv:
    query_string = f"""
    SELECT *
    FROM `{table_loc}.case_{series_len}h_hourly_labs`
    ORDER BY icustay_id, chart_time
    """

    df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    
    df.to_csv(f"output/case_{series_len}h_hourly_labs.csv", index=False)
    print(f"Create case_{series_len}h_hourly_labs.csv")

    query_string = f"""
    SELECT *
    FROM `{table_loc}.control_{series_len}h_hourly_labs`
    ORDER BY icustay_id, chart_time
    """

    df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    
    print(f"Create control_{series_len}h_hourly_labs.csv...")
    df.to_csv(f"output/control_{series_len}h_hourly_labs.csv", index=False)
    
    # Write vitals to csv:
    query_string = f"""
    SELECT *
    FROM `{table_loc}.case_{series_len}h_hourly_vitals`
    ORDER BY icustay_id, chart_time
    """

    df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    
    print(f"Create case_{series_len}h_hourly_vitals.csv...")
    df.to_csv(f"output/case_{series_len}h_hourly_vitals.csv", index=False)
    

    query_string = f"""
    SELECT *
    FROM `{table_loc}.control_{series_len}h_hourly_vitals`
    ORDER BY icustay_id, chart_time
    """

    df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    
    print(f"Create control_{series_len}h_hourly_vitals.csv...")
    df.to_csv(f"output/control_{series_len}h_hourly_vitals.csv", index=False)

def final_exclusion(window_hr, series_len):

    print(f"Excluding icustays with onset_hour <= {window_hr}...")

    static_case = pd.read_csv("output/static_variables_cases.csv")
    print(f"Number of icustays static_variables_cases before exclusion {static_case.shape[0]}")
    # set de-identified ages to median of 91.4
    static_case.loc[static_case.admission_age > 89, 'admission_age'] = 91.4
    # exclude cases with sepsis onset time within window_hr hours of icu admission
    static_case = static_case[static_case.sepsis_onset_hour > window_hr]
    n_case1 = static_case.shape
    print(f"Number of icustays static_variables_cases after exclusion {static_case.shape[0]}")
    static_case.to_csv(f"output/static_variables_cases_ex{window_hr}h.csv", index=False)

    static_control = pd.read_csv("output/static_variables_controls.csv")
    print(f"Number of icustays static_variables_controls before exclusion {static_control.shape[0]}")
    # set de-identified ages to median of 91.4
    static_control.loc[static_control.admission_age > 89, 'admission_age'] = 91.4
    # exclude controls with sepsis onset time within window_hr hours of icu admission
    static_control = static_control[static_control.control_onset_hour > window_hr]
    print(f"Number of icustays static_variables_controls after exclusion {static_control.shape[0]}")
    static_control.to_csv(f"output/static_variables_controls_ex{window_hr}h.csv", index=False)

    # drop 11 lab measurements had most missing
    DROP_LAB = [
        'ANIONGAP', 'Glucose_CSF', 'Total_Protein_Joint_Fluid',
        'Total_Protein_Pleural', 'Urine_Albumin_Creatinine_ratio',
        'WBC_Ascites', 'Ferritin', 'Transferrin', 'D_Dimer', 
        'SedimentationRate', 'NTproBNP'
    ]

    x = pd.read_csv(f"output/case_{series_len}h_hourly_labs.csv")
    y = x.groupby(['icustay_id', 'chart_time'], as_index=False).mean()
    case_lab = y.drop(columns=DROP_LAB)
    print(f"case_lab before exclusion: # icustays {case_lab.drop_duplicates('icustay_id').shape[0]}, # records {case_lab.shape[0]}")
    case_lab = pd.merge(case_lab, static_case[['icustay_id', 'sepsis_onset']], on='icustay_id', how='inner')
    case_lab['sepsis_onset']= pd.to_datetime(case_lab.sepsis_onset)
    case_lab['chart_time'] = pd.to_datetime(case_lab.chart_time)
    # hours before start of prediction window
    case_lab['hr_feature'] = (case_lab.sepsis_onset-case_lab.chart_time)/pd.Timedelta(hours=1) - window_hr
    # exclude records in prediction window
    case_lab = case_lab[case_lab.hr_feature > 0]
    print(f"case_lab after exclusion: # icustays {case_lab.drop_duplicates('icustay_id').shape[0]}, # records {case_lab.shape[0]}")
    case_lab.to_csv(f"output/case_{series_len}h_labs_ex{window_hr}h.csv", index=False)

    x = pd.read_csv(f"output/control_{series_len}h_hourly_labs.csv")
    y = x.groupby(['icustay_id', 'chart_time'], as_index=False).mean()
    control_lab = y.drop(columns=DROP_LAB)
    print(f"control_lab before exclusion: # icustays {control_lab.drop_duplicates('icustay_id').shape[0]}, # records {control_lab.shape[0]}")
    control_lab = pd.merge(control_lab, static_control[['icustay_id', 'control_onset_time']], on='icustay_id', how='inner')
    control_lab['control_onset_time']= pd.to_datetime(control_lab.control_onset_time)
    control_lab['chart_time'] = pd.to_datetime(control_lab.chart_time)
    # hours before start of prediction window
    control_lab['hr_feature'] = (control_lab.control_onset_time-control_lab.chart_time)/pd.Timedelta(hours=1) - window_hr
    # exclude records in prediction window
    control_lab = control_lab[control_lab.hr_feature > 0]
    print(f"control_lab after exclusion: # icustays {control_lab.drop_duplicates('icustay_id').shape[0]}, # records {control_lab.shape[0]}")
    control_lab.to_csv(f"output/control_{series_len}h_labs_ex{window_hr}h.csv", index=False)

    DROP_VITAL = [
        'VitalCapacity', 'TFC',          
        'TPR', 'Flowrate', 'SVI',
        'CRP', 'SV', 'SVV'    
    ]

    x = pd.read_csv(f"output/case_{series_len}h_hourly_vitals.csv")
    y = x.groupby(['icustay_id', 'chart_time'], as_index=False).mean()
    case_vital = y.drop(columns=DROP_VITAL)
    print(f"case_vital before exclusion: # icustays {case_vital.drop_duplicates('icustay_id').shape[0]}, # records {case_vital.shape[0]}")
    case_vital = pd.merge(case_vital, static_case[['icustay_id', 'sepsis_onset']], on='icustay_id', how='inner')
    case_vital['sepsis_onset']= pd.to_datetime(case_vital.sepsis_onset)
    case_vital['chart_time'] = pd.to_datetime(case_vital.chart_time)
    # hours before start of prediction window
    case_vital['hr_feature'] = (case_vital.sepsis_onset-case_vital.chart_time)/pd.Timedelta(hours=1) - window_hr
    # exclude records in prediction window
    case_vital = case_vital[case_vital.hr_feature > 0]
    print(f"case_vital after exclusion: # icustays {case_vital.drop_duplicates('icustay_id').shape[0]}, # records {case_vital.shape[0]}")
    case_vital.to_csv(f"output/case_{series_len}h_vitals_ex{window_hr}h.csv", index=False)

    x = pd.read_csv(f"output/control_{series_len}h_hourly_vitals.csv")
    y = x.groupby(['icustay_id', 'chart_time'], as_index=False).mean()
    control_vital = y.drop(columns=DROP_VITAL)
    print(f"control_vital before exclusion: # icustays {control_vital.drop_duplicates('icustay_id').shape[0]}, # records {control_vital.shape[0]}")
    control_vital = pd.merge(control_vital, static_control[['icustay_id', 'control_onset_time']], on='icustay_id', how='inner')
    control_vital['control_onset_time']= pd.to_datetime(control_vital.control_onset_time)
    control_vital['chart_time'] = pd.to_datetime(control_vital.chart_time)
    # hours before start of prediction window
    control_vital['hr_feature'] = (control_vital.control_onset_time-control_vital.chart_time)/pd.Timedelta(hours=1) - window_hr
    # exclude records in prediction window
    control_vital = control_vital[control_vital.hr_feature > 0]
    print(f"control_vital after exclusion: # icustays {control_vital.drop_duplicates('icustay_id').shape[0]}, # records {control_vital.shape[0]}")
    control_vital.to_csv(f"output/control_{series_len}h_vitals_ex{window_hr}h.csv", index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Post-process the exported output files from the SQL query <ENTER_NAME>")
    parser.add_argument("-c", "--credential",
                        help="Google credential json file name under current working directory")
    parser.add_argument("-t", "--tableloc",
                        help="Project and dataset name, e.g. cdc.project")
    parser.add_argument("-w", "--window", default=3, type=int,
                        help="Prediction window length (hour) before onset time, default is 3")
    parser.add_argument("-l", "--serieslen", default=48, type=int,
                        help="Vital and lab series length before onset time, default is 48")
    args = parser.parse_args()
    
    if args.credential:
        # GCP credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), args.credential)
        bqclient = bigquery.Client()
        download(args.tableloc, args.serieslen)
    
    final_exclusion(args.window, args.serieslen)