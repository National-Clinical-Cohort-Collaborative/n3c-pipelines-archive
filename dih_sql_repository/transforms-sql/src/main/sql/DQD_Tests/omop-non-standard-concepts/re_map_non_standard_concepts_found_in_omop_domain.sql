CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/omop-non-standard-concepts/re_map_non_standard_concepts_found_in_omop_domain` AS
    ---------counts of non-standard concepts found in OMOP CDM domains `ri.foundry.main.dataset.8202a233-3bbc-4f52-9bc9-2229cca64f98`
    SELECT 
    s.domain as omop_source_domain,
    s.omop_domain_concept_id as domain_concept_id,
    s.total_count,
    s.distinct_person_count,
    s.data_partner_id,
    s.concept_id,
    s.concept_name,
    s.domain_id as source_concept_domain_id,
    s.vocabulary_id as source_concept_vocabulary_id,
    s.concept_class_id as source_class_id,
    s.standard_concept as source_standard_concept,
    s.concept_code as source_concept_code,
    s.valid_start_date as source_valid_start_date,
    s.valid_end_date as source_valid_end_date, 
    s.invalid_reason as source_invalid_reason

  --- from the non-standard concepts found, identify the target concept id using the ""Maps to" relationship id - 
  ----set map_rank as 1 for this relationship id
  --- ===========================================
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
        , c2.standard_concept            as target_standard_concept
        , 1 as map_rank    
    FROM  `ri.foundry.main.dataset.8202a233-3bbc-4f52-9bc9-2229cca64f98` s
    LEFT JOIN `ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71` cr 
    ON s.concept_id = cr.concept_id_1 AND cr.relationship_id = 'Maps to' AND cr.invalid_reason IS NULL 
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c2
        ON cr.concept_id_2 = c2.concept_id
        AND c2.invalid_reason IS NULL -- invalid records will map to concept_id = 0

