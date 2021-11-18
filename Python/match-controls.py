'''
Summary: Script to match controls to cases based on icustay_ids and case/control ratio.
NOTE (11/1/21): Original input/output steps below have been modified to use BigQuery instead of local CSV files

Input: it takes a
    - cases csv, that links case icustay_id to sepsis_onset_hour (relative time after icu-intime in hours)
    - control csv listing control icustay_ids and corresponding icu-intime
Output:
    - matched_controls.csv that list controls the following way:
        icustay_id, control_onset_time, control_onset_hour, matched_case_icustay_id  
Detailed Description:
    1. Load Input files
    2. Determine Control vs Case ratio p (e.g. 10/1)
    3. Loop:
        For each case:
            randomly select (without repe) p controls as matched_controls
            For each selected control:
                append to result df: icustay_id, control_onset_time, control_onset_hour, matched_case_icustay_id
                (icustay_id of this control, the cases sepsis_onset_hour as control_onset_hour and the absolute time as control_onset_time, and the matched_case_icustay_id)
    4. return result df as output
'''
import pandas as pd
import numpy as np
import os
import random
import sys

from google.cloud import bigquery

# GCP credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "bdfh.json")
bqclient = bigquery.Client()

# rigorous mode that prevents controls to have shorter LOS than onset_hour
rigorous = False

random.seed(1)


def get_matched_controls():
    result = pd.DataFrame()
    #--------------------
    # 1. Load Input data
    # This step has been modified from https://github.com/BorgwardtLab/mgp-tcn
    #--------------------
    cases = bqclient.query("select * from cdcproject.BDFH.cases").result().to_dataframe()
    controls = bqclient.query("select * from cdcproject.BDFH.controls").result().to_dataframe()

    controls = controls.drop_duplicates() # drop duplicate rows (NOTE: it seems like there are no duplicates) 
    controls = controls.reset_index(drop=True) # resetting row index for aesthetic reasons

    case_ids = cases['icustay_id'].unique() # get unique ids
    control_ids = controls['icustay_id'].unique()

    #--------------------------------
    # 2. Determine Control/Case Ratio
    #--------------------------------
    ratio = len(control_ids)/float(len(case_ids))
    rf = int(np.floor(ratio)) # rf is the ratio floored, to receive the largest viable integer ratio

    #---------------------------------------------
    # 3. For each case match 'ratio-many' controls 
    #---------------------------------------------
    if rigorous:
        # Apply patient horizon filtering here before matching!
        min_length=7
        selected_cases = cases[cases['sepsis_onset_hour']>=min_length]
        # TODO for future work: improved matching here!
        sys.exit()
    else: # random matching without conditions
        controls_s = controls.iloc[np.random.permutation(len(controls))] # Shuffle controls dataframe rows, for random control selection

        for i, case_id in enumerate(case_ids):
            matched_controls = controls_s[(i*rf):(rf*(i+1))] # select the next batch of controls to match to current case
            matched_controls = matched_controls.drop(columns=['delta_score', 'sepsis_onset']) #drop unnecessary cols
            matched_controls['matched_case_icustay_id'] = case_id # so that each matched control can be mapped back to its matched case
            result = result.append(matched_controls, ignore_index=True)
        
        def random_onset_hour(los):
            return random.uniform(0.0, los)
        result['control_onset_hour'] = result['length_of_stay'].apply(random_onset_hour)
        result['control_onset_time'] = result['intime'] + result['control_onset_hour'].astype('timedelta64[h]')

    # Sanity Check:
    if len(result) != rf*len(cases):
        raise ValueError('Resulting matched_controls dataframe not as long as ratio * #cases!')    

    print('Number of Cases: {}'.format(len(case_ids)))
    print('Number of Controls: {}'.format(len(control_ids)))
    print('Matching Ratio: {}, floored: {}'.format(ratio, rf))

    #---------------------------------------------------------------------
    # 4. Return matched controls for next step (load BigQuery table)
    #---------------------------------------------------------------------
    return result

def load_matched_controls(df):
    table_id = "cdcproject.BDFH.matched_controls_hourly"

    # Reference: https://googleapis.dev/python/bigquery/latest/usage/pandas.html#load-a-pandas-dataframe-to-a-bigquery-table
    print("Loading table: {}...".format(table_id))
    job = bqclient.load_table_from_dataframe(df, table_id)
    job.result()  # Wait for the job to complete.

    table = bqclient.get_table(table_id)  
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def main():
    cc_matches = get_matched_controls()
    load_matched_controls(cc_matches)

if __name__ == '__main__':
    main()
