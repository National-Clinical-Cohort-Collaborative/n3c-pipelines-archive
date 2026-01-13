  CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/condition_occurrence` TBLPROPERTIES (foundry_transform_profiles = 'SHUFFLE_PARTITIONS_LARGE, EXECUTOR_MEMORY_MEDIUM') AS
       SELECT 
          concept.concept_id
        , concept.concept_name
        , concept.invalid_reason
        , dom.data_partner_id
        , dom.condition_source_concept_name as source_concept_name
        , dom.condition_source_concept_id as source_concept_id
        , dom.condition_source_value as source_value
        , 'condition_occurrence' as domain
    FROM `/UNITE/LDS/clean/condition_occurrence` dom
--    JOIN `/UNITE/OMOP Vocabularies/concept` concept
      JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` concept 
    ON (dom.condition_concept_id = concept.concept_id)
    WHERE concept.standard_concept IS NULL OR concept.standard_concept != 'S'