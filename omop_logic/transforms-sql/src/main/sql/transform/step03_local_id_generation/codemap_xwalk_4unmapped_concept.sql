CREATE TABLE `ri.foundry.main.dataset.504c654b-01bd-4285-9a98-961827d926ab` AS
    -- SELECT distinct
    -- r.unmapped_source_concept_id,
    -- r.source_concept_name,
    -- r.reason as source_reason,
    -- r.standard_concept as rescue_standard_concept,
    -- r.concept_id_1 as source_concept_id,
    -- --r.concept_id_2  --- this is the target concept
    -- r.relationship_id as source_relationship_id,
    -- c.concept_id as target_concept_id,
    -- c.concept_name as target_concept_name,
    -- c.domain_id as target_domain_id,
    -- c.vocabulary_id as target_vocabulary_id,
    -- c.concept_class_id as target_concept_class_id,
    -- c.standard_concept as target_standard_concept,
    -- c.concept_code as target_concept_code,
    -- c.valid_start_date as target_validate_start_date,
    -- c.valid_end_date as target_validate_end_Date,
    -- c.invalid_reason as target_invalid_reason
    -- FROM  `ri.foundry.main.dataset.5c426fdd-0f71-4d4f-a120-b9a3d68bebdd` r
    -- join  `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    -- on r.concept_id_2 = c.concept_id

    SELECT distinct 
    -----out of the 993,996 unmapped concepts, remap 37,566 rows-- rescue 37,566 concept_class_id
    --- if include the site domain id, the total rows are 993,996, i.e. if select dc.site_domain_id in this dataset
    dc.source_domain
    , dc.domain_concept_id
    , dc.source_concept_name as domain_concept_name
    , dc.reason
    , c.concept_id as source_concept_id
    , c.concept_name as source_concept_name
    , COALESCE(c2.concept_id, 0)  as target_concept_id
    , COALESCE(c2.concept_name, 'No matching concept') AS target_concept_name
    , c2.vocabulary_id AS target_vocabulary_id
    , c2.domain_id as target_domain_id
    , c2.invalid_reason AS target_invalid_reason
    , cr.relationship_id --- we are only using the "maps to" relationship
    FROM`ri.foundry.main.dataset.c647f4a5-1faa-4277-add5-8574c6f3e2e1` dc -- collected deprecated non-standard-concepts
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_id = dc.domain_concept_id AND (upper(c.vocabulary_id) = upper('SNOMED'))
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    ---- it was recommended that we do not use any other relationship_id without a manual review
    ---- remap using "Maps to" to see if we can rescue, as the "Maps to is the only reliable source to remap"
    LEFT JOIN  `ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71` cr 
        ON dc.domain_concept_id = cr.concept_id_1
        AND cr.invalid_reason IS NULL 
        AND lower(cr.relationship_id) in ( 'maps to', 'rxnorm is a', 'concept same_as to', 'concept replaced by', 'concept poss_eq to' ) --- should also add "RxNorm is a" relationship as well to rescue the umapped concepts
    LEFT JOIN  `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772`  c2
        ON cr.concept_id_2 = c2.concept_id
        AND c2.invalid_reason IS NULL -- invalid records will map to concept_id = 0


