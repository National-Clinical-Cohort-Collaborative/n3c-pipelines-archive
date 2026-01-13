CREATE TABLE `ri.foundry.main.dataset.4691f539-2a72-4abe-a1fa-d2f6a9cba939` AS

WITH site_codes AS ( 
    SELECT DISTINCT
        'OBSERVATION_FACT' as cdm_tbl, 
        f.parsed_concept_code,
        f.parsed_vocab_code,
        f.mapped_vocab_code
    FROM `ri.foundry.main.dataset.b45684f1-ba10-4a41-a49b-34a80dee15c4` f
    WHERE parsed_vocab_code not like 'DEM|%' 
    -- AND parsed_vocab_code not like 'VISIT|%' AND parsed_vocab_code not like 'DIST|%' -- Nothing being done with these codes
),

source_concept_mapping AS (
    SELECT DISTINCT
          site_codes.cdm_tbl
        , site_codes.parsed_concept_code    AS src_code
        , site_codes.parsed_vocab_code      AS src_code_type
        , site_codes.mapped_vocab_code      AS src_vocab_code
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
    FROM site_codes 
    LEFT JOIN `ri.foundry.main.dataset.82451eb6-4185-4ff6-9d0b-5ad33d89f3f4` c
        ON c.concept_code = site_codes.parsed_concept_code
        AND upper(c.vocabulary_id) = upper(site_codes.mapped_vocab_code)
        AND c.concept_class_id = 
            CASE 
                WHEN site_codes.parsed_vocab_code = 'DRG' THEN 'DRG' 
                WHEN site_codes.parsed_vocab_code = 'MSDRG' THEN 'MS-DRG'
                ELSE concept_class_id
            END
        AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
),

target_concept_mapping AS (
    SELECT DISTINCT
          source_concept_mapping.*
        , COALESCE(c2.concept_id, 0)     AS target_concept_id
        , COALESCE(
            c2.concept_name,
            'No matching concept')       AS target_concept_name
        , c2.vocabulary_id               AS target_vocabulary_id
        , COALESCE(
            c2.domain_id,
            'Observation')               AS target_domain_id
        , c2.concept_class_id            AS target_concept_class_id
        , c2.valid_start_date            AS target_valid_start_date
        , c2.valid_end_date              AS target_valid_end_date
        , c2.invalid_reason              AS target_invalid_reason
    FROM source_concept_mapping 
    LEFT JOIN `ri.foundry.main.marketplace-repository-integration.1303e19d-d66b-47fc-bb74-6797c1d61143_relationship` cr 
        ON source_concept_mapping.source_concept_id = cr.concept_id_1
        AND cr.invalid_reason IS NULL 
        AND lower(cr.relationship_id) = 'maps to'
    LEFT JOIN `ri.foundry.main.dataset.82451eb6-4185-4ff6-9d0b-5ad33d89f3f4` c2
        ON cr.concept_id_2 = c2.concept_id
        AND c2.invalid_reason IS NULL -- invalid records will map to concept_id = 0
)

SELECT DISTINCT * FROM target_concept_mapping
