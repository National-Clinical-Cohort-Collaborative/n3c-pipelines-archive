CREATE TABLE `ri.foundry.main.dataset.c647f4a5-1faa-4277-add5-8574c6f3e2e1` AS
      
      --null domain id / null concept id/ bring in other types that are not in Standard category
      -- observation
    SELECT
              observation_id as site_domain_id
            , 'OBSERVATION_ID:' || observation_id as source_pkey
            , person_id as site_person_id
            , o.observation_concept_id as domain_concept_id
            , source_concept_id as unmapped_source_concept_id
            , source_concept_name
            , CASE WHEN c.invalid_reason = 'D' THEN 'Deleted'
                WHEN c.invalid_reason = 'U' THEN 'Replaced with an update'
                WHEN c.invalid_reason is NULL THEN 'Valid_end_date has the default value'
                end as reason
            , c.valid_end_date
            ,'OBSERVATION' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.5f465bad-f62f-4fa7-832f-e8e8e653169b` o
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_id = o.observation_concept_id AND c.concept_class_id != 'ICD10PCS Hierarchy' AND vocabulary_id = 'SNOMED'
        WHERE observation_id IS NOT NULL 
        AND (o.target_concept_id is null -- deprecated concept_id will result in the null target_concept_id
            -- AND (o.target_domain_id IS NULL or o.target_domain_id NOT IN ('Measurement', 'Condition')) --- need to capture all other domain, unless we are mapping them to another domain
            -------observation domain contains data in the following domain -- obs, measurement, condition,  null note and visit visit) 
        or o.target_domain_id IS NULL) ----or o.target_domain_id NOT IN ( 'Observation', 'Measurement', 'Condition', 'Note', 'Visit')----- unmapped domain 
    -- condition
    UNION
    SELECT
              condition_occurrence_id as site_domain_id
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id as source_pkey
            , person_id as site_person_id
            , c.condition_concept_id as domain_concept_id
            , source_concept_id as unmapped_source_concept_id
            , source_concept_name
            , CASE WHEN c.invalid_reason = 'D' THEN 'Deleted'
                WHEN c.invalid_reason = 'U' THEN 'Replaced with an update'
                WHEN c.invalid_reason is NULL THEN 'Valid_end_date has the default value'
                end as reason
            , c.valid_end_date
            ,'CONDITION' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.2ccc3011-db3a-47de-b6fe-4fc0908129c8` c
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_id = c.condition_concept_id AND c.concept_class_id != 'ICD10PCS Hierarchy' AND vocabulary_id = 'SNOMED'
        WHERE condition_occurrence_id IS NOT NULL 
        AND (c.target_concept_id is null OR c.target_domain_id IS NULL) ----- unmapped domain 
        -- drug
    UNION
    SELECT
              drug_exposure_id as site_domain_id
            , 'DRUG_EXPOSURE_ID:' || drug_exposure_id as source_pkey
            , person_id as site_person_id
            , d.drug_concept_id as domain_concept_id
            , source_concept_id as unmapped_source_concept_id
            , source_concept_name
            , CASE WHEN c.invalid_reason = 'D' THEN 'Deleted'
                WHEN c.invalid_reason = 'U' THEN 'Replaced with an update'
                WHEN c.invalid_reason is NULL THEN 'Valid_end_date has the default value'
                end as reason
            , c.valid_end_date
            ,'DRUG' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.0e99bf0a-15ec-44db-b71d-8b711c8eab3a` d
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_id = d.drug_concept_id AND c.concept_class_id != 'ICD10PCS Hierarchy' AND vocabulary_id = 'SNOMED'
        WHERE drug_exposure_id IS NOT NULL 
        AND (d.target_concept_id is null OR d.target_domain_id IS NULL) ----- unmapped domain        
        -- measurement
    UNION
    SELECT
              measurement_id as site_domain_id
            , 'MEASUREMENT_ID:' || measurement_id as source_pkey
            , person_id as site_person_id
            , m.measurement_concept_id as domain_concept_id
            , source_concept_id as unmapped_source_concept_id
            , source_concept_name
            , CASE WHEN c.invalid_reason = 'D' THEN 'Deleted'
                WHEN c.invalid_reason = 'U' THEN 'Replaced with an update'
                WHEN c.invalid_reason is NULL THEN 'Valid_end_date has the default value'
                end as reason
            , c.valid_end_date
            ,'MEASUREMENT' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.d36f1ed4-2d1f-42c0-a6d4-183c77d8cf3e` m
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_id = m.measurement_concept_id AND c.concept_class_id != 'ICD10PCS Hierarchy' AND vocabulary_id = 'SNOMED'
        WHERE measurement_id IS NOT NULL 
        AND (m.target_concept_id is null OR m.target_domain_id IS NULL) ----- unmapped domain       
        -- procedure
    UNION
    SELECT
              procedure_occurrence_id as site_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id as source_pkey
            , person_id as site_person_id
            , p.procedure_concept_id as domain_concept_id
            , source_concept_id as unmapped_source_concept_id
            , source_concept_name
            , CASE WHEN c.invalid_reason = 'D' THEN 'Deleted'
                WHEN c.invalid_reason = 'U' THEN 'Replaced with an update'
                WHEN c.invalid_reason is NULL THEN 'Valid_end_date has the default value'
                end as reason
            , c.valid_end_date
            ,'PROCEDURE' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.8e9c4a3f-516e-4288-8ec5-946c787ddb5a` p
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_id = p.procedure_concept_id AND c.concept_class_id != 'ICD10PCS Hierarchy' AND vocabulary_id = 'SNOMED'
        WHERE procedure_occurrence_id IS NOT NULL 
        AND (p.target_concept_id is null OR p.target_domain_id IS NULL) ----- unmapped domain    
        --device
    UNION
    select 
              device_exposure_id as site_domain_id
            , 'DEVICE_EXPOSURE_ID:' || device_exposure_id as source_pkey
            , person_id as site_person_id
            , d.device_concept_id as domain_concept_id
            , source_concept_id as unmapped_source_concept_id
            , source_concept_name
            , CASE WHEN c.invalid_reason = 'D' THEN 'Deleted'
                WHEN c.invalid_reason = 'U' THEN 'Replaced with an update'
                WHEN c.invalid_reason is NULL THEN 'Valid_end_date has the default value'
                end as reason
            , c.valid_end_date
            ,'DEVICE' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.de40ba11-7679-45fe-a34c-2fb986a989a7` d
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_id = d.device_concept_id AND c.concept_class_id != 'ICD10PCS Hierarchy' AND vocabulary_id = 'SNOMED'
        WHERE device_concept_id IS NOT NULL 
        AND (d.target_concept_id is null OR d.target_domain_id IS NULL) ----- unmapped domain  

