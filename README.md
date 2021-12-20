# sepsis-detection

This repository is initially developed for a group project from a Gatech OMSCS class. The version delivered during the class is tagged as v0.1.   

## Data

In order to access MIMIC-III database on Google Clould, please follow the instructions in https://physionet.org/content/mimiciii/1.4/.

Run sql and python code in following orders to recreate the csv files used in modeling.

```
Cohort.sql
hourly_cohort.sql
static-query.sql
match-control.py
extract-48h-of-hourly-case-lab-series.sql
extract-48h-of-hourly-case-vital-series.sql
extract-48h-of-hourly-control-lab-series.sql
extract-48h-of-hourly-control-vital-series.sql
data_prep_step1.py
```

All sql codes can be run on GCP BigQuery console by changing project and data names to your own location accordingly. Althernatively, one can run the first three queries with `get_cohort.sh` on clould shell with project id as the lone argument. Similarly, the other four sequence queries can be run with `extract_sequence.sh`.

The two python files can be run as follows assuming you are in the top level folder
```
python Python/match-control.py -c <bigquery credential json filename> -t <bigquery table reference>
python Python/data_prep_step1.py -c <bigquery credential json filename> -t <bigquery table reference> -w <prediction window hours(default is 3)>
```
and an example would be
```
python Python/match-control.py -c bdfh.json -t cdcproject.BDFH
python Python/data_prep_step1.py -c bdfh.json -t cdcproject.BDFH -w 3
```
The data will be saved in the Data folder. 

Note: while we tried to set the random seed whenever possible for reproducibility, there still might be some factors we overlooked which might cause differences in model input data.

## Models

* Logistic regression: trained and evaluated in LR.ipynb
* SVM: trained and evaluated in SVM_Model.ipynb
* LightGBM: trained and evaluated in Lightgbm_Model.ipynb
* RNN: trained using `python Python/rnn_main.py`, evaluated in RNN_evaluation.ipynb

## Resource

* Data pipeline: Detailed data filtering and label definition: Early Recognition of Sepsis with Gaussian Process Temporal
Convolutional Networks and Dynamic Time Warping, [paper](http://proceedings.mlr.press/v106/moor19a/moor19a.pdf), [code](https://github.com/BorgwardtLab/mgp-tcn/tree/master/src/query)
* Original sql codes based on MIMIC-III can be found in https://github.com/MIT-LCP/mimic-code
* Reference library: https://www.zotero.org/groups/4456592/bdfh_project/items/7JQNQF39/library