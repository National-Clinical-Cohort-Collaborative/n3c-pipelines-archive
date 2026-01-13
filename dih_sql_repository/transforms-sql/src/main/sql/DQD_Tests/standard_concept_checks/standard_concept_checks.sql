CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/standard_concept_checks` TBLPROPERTIES (foundry_transform_profiles = 'NUM_EXECUTORS_8, EXECUTOR_MEMORY_MEDIUM') AS

    SELECT 
          concept.concept_id
        , concept.concept_name
        , concept.invalid_reason
        , domain.data_partner_id
        , domain.measurement_source_concept_name as source_concept_name
        , domain.measurement_source_concept_id as source_concept_id
        , domain.measurement_source_value as source_value
        , 'measurement' as domain
    FROM `/UNITE/LDS/clean/measurement` domain
--    LEFT JOIN `/UNITE/OMOP Vocabularies/concept` concept
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` concept
    ON (domain.measurement_concept_id = concept.concept_id)
    WHERE concept.standard_concept IS NULL OR concept.standard_concept != 'S'

    UNION ALL

    SELECT 
          concept.concept_id
        , concept.concept_name
        , concept.invalid_reason
        , domain.data_partner_id
        , domain.condition_source_concept_name as source_concept_name
        , domain.condition_source_concept_id as source_concept_id
        , domain.condition_source_value as source_value
        , 'condition_occurrence' as domain
    FROM `/UNITE/LDS/clean/condition_occurrence` domain
--    LEFT JOIN `/UNITE/OMOP Vocabularies/concept` concept
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` concept
    ON (domain.condition_concept_id = concept.concept_id)
    WHERE concept.standard_concept IS NULL OR concept.standard_concept != 'S'

    UNION ALL

    SELECT 
          concept.concept_id
        , concept.concept_name
        , concept.invalid_reason
        , domain.data_partner_id
        , domain.drug_source_concept_name as source_concept_name
        , domain.drug_source_concept_id as source_concept_id
        , domain.drug_source_value as source_value
        , 'drug_exposure' as domain
    FROM `/UNITE/LDS/clean/drug_exposure` domain
--    LEFT JOIN `/UNITE/OMOP Vocabularies/concept` concept
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` concept
    ON (domain.drug_concept_id = concept.concept_id)
    WHERE concept.standard_concept IS NULL OR concept.standard_concept != 'S'

    UNION ALL

    SELECT 
          concept.concept_id
        , concept.concept_name
        , concept.invalid_reason
        , domain.data_partner_id
        , domain.observation_source_concept_name as source_concept_name
        , domain.observation_source_concept_id as source_concept_id
        , domain.observation_source_value as source_value
        , 'observation' as domain
    FROM `/UNITE/LDS/clean/observation` domain
--    LEFT JOIN `/UNITE/OMOP Vocabularies/concept` concept
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` concept
    ON (domain.observation_concept_id = concept.concept_id)
    WHERE concept.standard_concept IS NULL OR concept.standard_concept != 'S'

    UNION ALL

    SELECT 
          concept.concept_id
        , concept.concept_name
        , concept.invalid_reason
        , domain.data_partner_id
        , domain.procedure_source_concept_name as source_concept_name
        , domain.procedure_source_concept_id as source_concept_id
        , domain.procedure_source_value as source_value
        , 'procedure_occurrence' as domain
    FROM `/UNITE/LDS/clean/procedure_occurrence` domain
--    LEFT JOIN `/UNITE/OMOP Vocabularies/concept` concept
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` concept
    ON (domain.procedure_concept_id = concept.concept_id)
    WHERE concept.standard_concept IS NULL OR concept.standard_concept != 'S'

    UNION ALL

    SELECT 
          concept.concept_id
        , concept.concept_name
        , concept.invalid_reason
        , domain.data_partner_id
        , domain.visit_source_concept_name as source_concept_name
        , domain.visit_source_concept_id as source_concept_id
        , domain.visit_source_value as source_value
        , 'visit_occurrence' as domain
    FROM `/UNITE/LDS/clean/visit_occurrence` domain
--    LEFT JOIN `/UNITE/OMOP Vocabularies/concept` concept
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` concept
    ON (domain.visit_concept_id = concept.concept_id)
    WHERE concept.standard_concept IS NULL OR concept.standard_concept != 'S'
