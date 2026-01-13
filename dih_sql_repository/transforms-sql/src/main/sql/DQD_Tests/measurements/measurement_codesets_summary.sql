
-- Group the measurement summaries by concept_set with a short descriptive name.
-- The choices were made from selected possible concept sets, and associated with a name in a workbook.
-- A table here has the result of that work, including the list of concept sets consider for each
-- group of candidate concept sets.

CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/measurement_codesets_summary` AS

WITH A as (
SELECT mcs.data_partner_id, ccs.Subgroup, ccs.Display, -- csm.codeset_id, csm.concept_set_name, 
    sum(CASE WHEN (numeric_value_qc_check = 'TOO LOW') THEN mcs.count 
             WHEN (numeric_value_qc_check = 'TOO HIGH') THEN mcs.count  
             ELSE 0 
        END) as error_count_sum, 
    sum(CASE WHEN (numeric_value_qc_check = 'TOO LOW') THEN mcs.count 
             ELSE 0 
        END) as low_count_sum, 
    sum(CASE  WHEN (numeric_value_qc_check = 'TOO HIGH') THEN mcs.count 
             ELSE 0 
        END) as high_count_sum, 
    sum(CASE  WHEN (numeric_value_qc_check = 'NULL VALUE') THEN mcs.count 
             ELSE 0 
        END) as null_count_sum,
    sum(CASE WHEN (numeric_value_qc_check = 'TOO LOW') THEN mcs.patient_count 
             WHEN (numeric_value_qc_check = 'TOO HIGH') THEN mcs.patient_count  
             ELSE 0 
        END) as error_patient_count_sum, 
    sum(CASE WHEN (numeric_value_qc_check = 'TOO LOW') THEN mcs.patient_count 
             ELSE 0 
        END) as low_patient_count_sum, 
    sum(CASE  WHEN (numeric_value_qc_check = 'TOO HIGH') THEN mcs.patient_count 
             ELSE 0 
        END) as high_patient_count_sum, 
    sum(CASE  WHEN (numeric_value_qc_check = 'NULL VALUE') THEN mcs.patient_count 
             ELSE 0 
        END) as null_patient_count_sum, 
    sum(mcs.count) as all_count_sum, 
    sum(mcs.patient_count) as all_patient_count_sum, 
    count(*) as con_count
FROM `ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6` csm
JOIN `ri.foundry.main.dataset.ce02ebe8-1cde-40e6-b744-b13b6a71e5ad` mcs 
  on mcs.measurement_concept_id = csm.concept_id
 JOIN `ri.foundry.main.dataset.835cac88-a9f3-4dac-9e1e-e9dc05577c34` ccs
  on csm.codeset_id == ccs.conceptset_id
WHERE is_most_recent_version is true 
  AND ccs.naive_choice = true
GROUP BY Subgroup, Display, data_partner_id -- ,csm.codeset_id, csm.concept_set_name 
ORDER BY data_partner_id, Subgroup, error_count_sum desc 
)
select Subgroup, Display, data_partner_id,
low_count_sum, high_count_sum, error_count_sum, null_count_sum, all_count_sum, 
error_count_sum / all_count_sum * 100 as row_percent, 
low_patient_count_sum, high_patient_count_sum, error_patient_count_sum,  null_patient_count_sum, all_patient_count_sum, 
error_patient_count_sum / all_patient_count_sum  * 100 as patient_percent
from A

