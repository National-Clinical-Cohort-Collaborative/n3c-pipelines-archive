CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/omop-non-standard-concepts/non-standard-concepts_from_aggregated_domain` TBLPROPERTIES (foundry_transform_profiles = 'SHUFFLE_PARTITIONS_LARGE, EXECUTOR_MEMORY_LARGE') AS
--- filtered to omop and non-standard only from all domains with concept ids
       SELECT DISTINCT
       domain
        , concept_id
        , concept_name
        , standard_concept
        , invalid_reason
           , CASE WHEN invalid_reason = 'D' THEN 'Deleted'
                WHEN invalid_reason = 'U' THEN 'Replaced with an update'
                WHEN invalid_reason is NULL THEN 'Valid_end_date has the default value'
                end as reason
        , concept_code
        , vocabulary_id
        , domain_id

        ------------------, domain_source_value may want to include this when building from each domain data with pkey
    --- build from term usage aggregated dataset 
    FROM `ri.foundry.main.dataset.8202a233-3bbc-4f52-9bc9-2229cca64f98` dom
    
    -- UNION ALL

    -- SELECT 
    --       concept.concept_id
    --     , concept.concept_name
    --     , concept.invalid_reason
    --     , domain.data_partner_id
    --     , domain.measurement_source_concept_name as source_concept_name
    --     , domain.measurement_source_concept_id as source_concept_id
    --     , domain.measurement_source_value as source_value
    --     , 'measurement' as source_domain
    -- FROM `/UNITE/LDS Release/datasets/measurement` domain
   
    -- UNION ALL

    -- SELECT 
    --       concept.concept_id
    --     , concept.concept_name
    --     , concept.invalid_reason
    --     , domain.data_partner_id
    --     , domain.condition_source_concept_name as source_concept_name
    --     , domain.condition_source_concept_id as source_concept_id
    --     , domain.condition_source_value as source_value
    --     , 'condition_occurrence' as source_domain
    -- FROM `/UNITE/LDS Release/datasets/condition_occurrence` domain
    
    -- UNION ALL

    -- SELECT 
    --       concept.concept_id
    --     , concept.concept_name
    --     , concept.invalid_reason
    --     , domain.data_partner_id
    --     , domain.drug_source_concept_name as source_concept_name
    --     , domain.drug_source_concept_id as source_concept_id
    --     , domain.drug_source_value as source_value
    --     , 'drug_exposure' as source_domain
    -- FROM `/UNITE/LDS Release/datasets/drug_exposure` domain

    -- UNION ALL

    -- SELECT 
    --       concept.concept_id
    --     , concept.concept_name
    --     , concept.invalid_reason
    --     , domain.data_partner_id
    --     , domain.observation_source_concept_name as source_concept_name
    --     , domain.observation_source_concept_id as source_concept_id
    --     , domain.observation_source_value as source_value
    --     , 'observation' as source_domain
    -- FROM `/UNITE/LDS Release/datasets/observation` domain
    
    -- UNION ALL

    -- SELECT 
    --       concept.concept_id
    --     , concept.concept_name
    --     , concept.invalid_reason
    --     , domain.data_partner_id
    --     , domain.procedure_source_concept_name as source_concept_name
    --     , domain.procedure_source_concept_id as source_concept_id
    --     , domain.procedure_source_value as source_value
    --     , 'procedure_occurrence' as source_domain
    -- FROM `/UNITE/LDS Release/datasets/procedure_occurrence` domain
   
    -- UNION ALL

    -- SELECT 
    --       dom.visit_concept_id as concept_id
    --     , concept.concept_name
    --     , concept.invalid_reason
    --     , domain.data_partner_id
    --     , domain.visit_source_concept_name as source_concept_name
    --     , domain.visit_source_concept_id as source_concept_id
    --     , domain.visit_source_value as source_value
    --     , 'visit_occurrence' as source_domain
    -- FROM `/UNITE/LDS Release/datasets/visit_occurrence` domain
  --)

-- select distinct 
--     c.concept_id
--         , concept.concept_name
--         , concept.invalid_reason
--         , source_concept_name
--         , source_concept_id
--         , source_value
--         , source_domain
--         , data_partner_id
--   from domain_concepts dom
--   JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
--     ON (dom.concept_id = c.concept_id)
--     WHERE c.standard_concept IS NULL OR c.standard_concept != 'S'