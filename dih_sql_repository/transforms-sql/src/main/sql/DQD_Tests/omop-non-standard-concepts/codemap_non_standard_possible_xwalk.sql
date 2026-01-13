CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/omop-non-standard-concepts/codemap_non_standard_possible_xwalk` AS
    -- select distinct non-standard code, see if standard concept can be found using the following relationship_ids
    ---with non_standard_domain_concept as (
    SELECT DISTINCT
    domain as source_domain
    ,s.concept_id as source_concept_id
    ,s.concept_name as source_concept_name
    -- ,s.standard_concept as source_standard_concept
    -- ,s.invalid_reason as source_invalid_reason
    -- ,s.reason as source_reason
    -- ,s.concept_code as source_concept_code
    -- ,s.vocabulary_id as source_vocabulary_id
    -- ,s.domain_id as source_domain_id
    --- target concept from maps to 
   ------ if the domain id is missing should we add them to the observation in order not to drop any data
        ,COALESCE(c2.concept_id, 0)     AS target_concept_id -- setting nulls to 0 
        , COALESCE(c2.concept_name, 'No matching concept') AS target_concept_name
        , c2.vocabulary_id               AS target_vocabulary_id
       --- , COALESCE( c2.domain_id, 'Observation')               AS target_domain_id
        , c2.domain_id as target_domain_id -- it is possible that the domain id is missing or null 
        , c2.concept_class_id            AS target_concept_class_id
        , c2.valid_start_date            AS target_valid_start_date
        , c2.valid_end_date              AS target_valid_end_date
        , c2.invalid_reason              AS target_invalid_reason
        , 1 as map_rank  
    FROM  `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/omop-non-standard-concepts/non-standard-concepts_from_aggregated_domain` s
    LEFT JOIN `ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71` cr 
    ON s.concept_id = cr.concept_id_1 AND cr.relationship_id = 'Maps to' AND cr.invalid_reason IS NULL 
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c2
        ON cr.concept_id_2 = c2.concept_id
        AND c2.invalid_reason IS NULL -- invalid records will map to concept_id = 0
