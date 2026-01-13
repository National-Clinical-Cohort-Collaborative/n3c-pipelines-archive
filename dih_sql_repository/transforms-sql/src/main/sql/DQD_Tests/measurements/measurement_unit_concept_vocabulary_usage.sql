CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/measurement_unit_concept_vocabulary_usage` TBLPROPERTIES (foundry_transform_profiles = 'NUM_EXECUTORS_4, EXECUTOR_MEMORY_SMALL') AS

-- Find where the units come from

SELECT m.data_partner_id, c.vocabulary_id, COUNT(*) AS row_count
FROM
    `/UNITE/LDS/clean/measurement` m
    -- LEFT JOIN `/UNITE/OMOP Vocabularies/concept` c
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON m.unit_concept_id = c.concept_id

GROUP BY    data_partner_id,
            vocabulary_id