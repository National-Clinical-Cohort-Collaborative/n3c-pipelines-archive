
-- summarize the tests dataset once so repeated queries don't duplicate that

CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/measurement_concept_summary` AS

SELECT data_partner_id, measurement_concept_id, count(*) as count, count(distinct person_id) as patient_count,  mt.numeric_value_qc_check
FROM `ri.foundry.main.dataset.645e71d0-c348-4ae4-a7fc-94460cf24159` mt
GROUP BY data_partner_id, measurement_concept_id, numeric_value_qc_check