CREATE TABLE `ri.foundry.main.dataset.97386241-9e4d-43cb-a7f8-c34b2c636234` AS

SELECT 
    patient_num as site_patient_num,
    CAST(death_date as date) as death_date,
    CAST(null as timestamp) as death_datetime,
    32510 as death_type_concept_id,
    0 as cause_concept_id,  -- there is no death cause as concept id , no death cause in ACT
    CAST(null as string) as cause_source_value, --put raw ICD10 codes here, no death cause in ACT 
    0 as cause_source_concept_id,  -- this field is number, ICD codes don't fit
    'PATIENT_DIMENSION' as domain_source, 
    data_partner_id,
    payload
FROM `ri.foundry.main.dataset.898923b2-c392-4633-9016-c03748e49dad` d
WHERE 
    (d.death_date is not null) OR (d.vital_status_cd in ('D', 'DEM|VITAL STATUS:D'))
