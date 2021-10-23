
--WBC Count
select SUBJECT_ID, HADM_ID, CHARTTIME, VALUENUM
from `physionet-data.mimiciii_clinical.labevents`
where ITEMID IN (
    (select ITEMID
     from `physionet-data.mimiciii_clinical.d_labitems`
     where LABEL IN ('WBC', 'WBC Count'))
) AND VALUE IS NOT NULL;

--Creatinine
select SUBJECT_ID, HADM_ID, CHARTTIME, VALUENUM
from `physionet-data.mimiciii_clinical.labevents`
where ITEMID IN (
    (select ITEMID
     from `physionet-data.mimiciii_clinical.d_labitems`
     where LABEL IN ('Creatinine', '24 hr Creatinine'))
) AND VALUE IS NOT NULL;

--Anion gap
select SUBJECT_ID, CHARTTIME, VALUENUM
from `physionet-data.mimiciii_clinical.labevents`
where ITEMID IN (
    (select ITEMID
     from `physionet-data.mimiciii_clinical.d_labitems`
     where LABEL IN ('Anion Gap'))
) AND VALUE IS NOT NULL;

--sodium
select SUBJECT_ID, CHARTTIME, VALUENUM
from `physionet-data.mimiciii_clinical.labevents`
where ITEMID IN (
    (select ITEMID
     from `physionet-data.mimiciii_clinical.d_labitems`
     where LABEL IN ('Sodium', 'Sodium, Whole Blood'))
) AND VALUE IS NOT NULL;

--hemoglobin
select SUBJECT_ID, CHARTTIME, VALUENUM
from `physionet-data.mimiciii_clinical.labevents`
where ITEMID IN (
    (select ITEMID
     from `physionet-data.mimiciii_clinical.d_labitems`
     where LABEL IN ('Hemoglobin'))
) AND VALUE IS NOT NULL;

--lactate
select SUBJECT_ID, CHARTTIME, VALUENUM
from `physionet-data.mimiciii_clinical.labevents`
where ITEMID IN (
    (select ITEMID
     from `physionet-data.mimiciii_clinical.d_labitems`
     where LABEL IN ('Lactate'))
) AND VALUE IS NOT NULL;

--potassium
select SUBJECT_ID, CHARTTIME, VALUENUM
from `physionet-data.mimiciii_clinical.labevents`
where ITEMID IN (
    (select ITEMID
     from `physionet-data.mimiciii_clinical.d_labitems`
     where LABEL IN ('Potassium', 'Potassium, Whole Blood'))
) AND VALUENUM IS NOT NULL;

--Urea Nitrogen
select SUBJECT_ID, CHARTTIME, VALUENUM
from `physionet-data.mimiciii_clinical.labevents`
where ITEMID IN (
    (select ITEMID
     from `physionet-data.mimiciii_clinical.d_labitems`
     where LABEL IN ('Urea Nitrogen'))
) AND VALUENUM IS NOT NULL;

--Glucose
select SUBJECT_ID, CHARTTIME, VALUENUM
from `physionet-data.mimiciii_clinical.labevents`
where ITEMID IN (
    (select ITEMID
     from `physionet-data.mimiciii_clinical.d_labitems`
     where LABEL IN ('Glucose'))
) AND VALUENUM IS NOT NULL;

--Event measures
select SUBJECT_ID, CHARTTIME, VALUENUM, LABEL
from `physionet-data.mimiciii_clinical.chartevents` a
INNER JOIN (
    select ITEMID, LABEL
    from `physionet-data.mimiciii_clinical.d_items`
    where LABEL IN (
        'Heart Rate', 
        'ABP [Systolic]',
        'ABP [Diastolic]',
        'Arterial BP [Systolic]',
        'Arterial BP [Diastolic]',
        'Respiratory Rate', 
        'Temperature C',
        'Temperature C (calc)',
        'SpO2'
        )
    ) b 
on a.ITEMID = b.ITEMID
WHERE VALUENUM IS NOT NULL 

-- select ITEMID, LABEL
-- from `physionet-data.mimiciii_clinical.d_items`
-- where LOWER(LABEL) LIKE '%diastolic%'

-- select ITEMID, LABEL
-- from `physionet-data.mimiciii_clinical.d_labitems`
-- where LOWER(LABEL) LIKE '%glucose%'

