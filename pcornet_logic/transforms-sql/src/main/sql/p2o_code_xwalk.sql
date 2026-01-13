CREATE TABLE `ri.foundry.main.dataset.05c59431-1c54-4ef8-a475-b3772d8e830f` AS

WITH site_codes_to_source_concept_mapping AS (
    SELECT DISTINCT
        'DIAGNOSIS' as CDM_TBL, 
        d.dx as src_code, 
        d.dx_type as src_code_type,
        d.mapped_dx_type as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `ri.foundry.main.dataset.eafe63b1-42db-4f3f-ac2f-0b96f91c5260` d
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_code = d.dx
            AND upper(c.vocabulary_id) = upper(d.mapped_dx_type)
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'PROCEDURES' as CDM_TBL, 
        p.px as src_code, 
        p.px_type as src_code_type,
        p.mapped_px_type as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `ri.foundry.main.dataset.da113963-4e92-42a2-9477-2591001cfcc1` p
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_code = p.px  
            AND (upper(c.vocabulary_id) = upper(p.mapped_px_type) 
                OR 
                -- Look for either CPT4 or HCPCS codes when px_type is 'CH'
                (p.px_type = 'CH' AND c.vocabulary_id in ('CPT4', 'HCPCS')))
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'CONDITION' as CDM_TBL, 
        cd.condition as src_code, 
        cd.condition_type as src_code_type, 
        cd.mapped_condition_type as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `ri.foundry.main.dataset.0c17b2c1-862d-4103-9c4d-25507bcc13e4` cd
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_code = cd.condition
            AND upper(c.vocabulary_id) = upper(cd.mapped_condition_type)
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'DEATH_CAUSE' as CDM_TBL,
        dt.death_cause as src_code,
        dt.death_cause_code as src_code_type, 
        dt.mapped_death_cause_code as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `ri.foundry.main.dataset.781f51c6-3213-41a6-9764-292238ae9866`dt
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_code = dt.death_cause
            AND upper(c.vocabulary_id) = upper(dt.mapped_death_cause_code)
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'LAB_RESULT_CM' as CDM_TBL,
        l.lab_loinc as src_code,
        'LOINC' as src_code_type,
        'LOINC' as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `ri.foundry.main.dataset.634e56ef-35da-4cbb-9290-cf8950708c92` l
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            -- Only looking up records using the lab_loinc field, so filter to LOINC vocab
            ON c.concept_code = l.lab_loinc
            AND c.vocabulary_id = 'LOINC'
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'DISPENSING' as CDM_TBL,
        dx.ndc as src_code,
        'NDC' as src_code_type,
        'NDC' as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `ri.foundry.main.dataset.5f3a9356-809c-465d-8725-f52dd918bd3c` dx
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            -- Only looking up records using the ndc field, so filter to NDC vocab
            ON c.concept_code=dx.ndc
            AND c.vocabulary_id = 'NDC'
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'MED_ADMIN' as CDM_TBL,
        m.medadmin_code as src_code,
        m.medadmin_type as src_code_type, 
        m.mapped_medadmin_type as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `ri.foundry.main.dataset.4b6a2c8d-b352-4574-9861-3bfc764b0900` m
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_code =  m.medadmin_code
            AND upper(c.vocabulary_id) = upper(m.mapped_medadmin_type)
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'PRESCRIBING' as CDM_TBL,
        p.rxnorm_cui as src_code,
        'rxnorm_cui' as src_code_type,
        'RxNorm' as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `ri.foundry.main.dataset.97e347ba-1a02-492f-8443-f0859a898389` p
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_code = p.rxnorm_cui
            -- Only looking up records using the rxnorm_cui field, so filter to RxNorm and RxNorm Extension vocabs
            -- (There's no overlap in concept_codes between the RxNorm and RxNorm Extension vocabs, so no risk of multi-mapping)
            AND c.vocabulary_id in ('RxNorm', 'RxNorm Extension')
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'OBS_CLIN' as CDM_TBL,
        o.obsclin_code as src_code,
        o.obsclin_type as src_code_type, 
        o.mapped_obsclin_type as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `ri.foundry.main.dataset.3dacc398-2418-4bd9-9dac-4a67a1e000ad` o
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_code = o.obsclin_code
            AND upper(c.vocabulary_id) = upper(o.mapped_obsclin_type)
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'IMMUNIZATION' as CDM_TBL, 
        i.vx_code as src_code, 
        i.vx_code_type as src_code_type,
        i.mapped_vx_code_type as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `ri.foundry.main.dataset.858a2339-3e51-4954-ba88-149584aab79d` i
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_code = i.vx_code
            -- Look for either CPT4 or HCPCS codes when vx_code_type is 'CH'
            AND ((upper(c.vocabulary_id) = upper(i.mapped_vx_code_type)) 
                OR 
                (i.vx_code_type = 'CH' AND c.vocabulary_id in ('CPT4' 'HCPCS')))
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT  
        'PRO_CM' as CDM_TBL
        ,pro.pro_item_loinc as src_code
        ,pro.pro_type as src_code_type
        ,'LOINC' as src_vocab_code
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `ri.foundry.main.dataset.7769b85a-98eb-473b-a414-843d7c7ffde3` pro
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_code = pro.pro_item_loinc AND (upper(c.vocabulary_id) = upper('LOINC'))
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
        where pro.pro_type = 'LC'
    UNION ALL
    SELECT DISTINCT  
        'PRO_CM' as CDM_TBL
        ,pro.pro_measure_loinc as src_code
        ,pro.pro_type as src_code_type
        ,'LOINC' as src_vocab_code
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `ri.foundry.main.dataset.7769b85a-98eb-473b-a414-843d7c7ffde3` pro
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_code = pro.pro_measure_loinc AND (upper(c.vocabulary_id) = upper('LOINC'))
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
        where pro.pro_type = 'LC'
    UNION ALL
    -- there are LOINC codes coming in via the OBS_GEN
    select 
        'OBS_GEN' as CDM_TBL
        ,ob.obsgen_code as src_code
        ,ob.obsgen_type as src_code_type
        ,'LOINC' as src_vocab_code -- 'LC' types are LOINC
        ---i.mapped_vx_code_type as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `ri.foundry.main.dataset.b52c4215-d873-4931-a8a8-455282ec9a06` ob
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_code = ob.obsgen_code AND (upper(c.vocabulary_id) = upper('LOINC'))
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
        where ob.obsgen_type = 'LC'
    UNION ALL 
    -- if there are SNOMED CT codes coming in via the OBS_GEN
    select 
        'OBS_GEN' as CDM_TBL
        ,ob.obsgen_code as src_code
        ,ob.obsgen_type as src_code_type
        ,'SNOMED' as src_vocab_code -- 'SM' types are SNOMED
        ---i.mapped_vx_code_type as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `ri.foundry.main.dataset.b52c4215-d873-4931-a8a8-455282ec9a06` ob
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON c.concept_code = ob.obsgen_code AND (upper(c.vocabulary_id) = upper('SNOMED'))
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
        where ob.obsgen_type = 'SM'             

),

target_concept_mapping AS (
    SELECT DISTINCT
          site_codes_to_source_concept_mapping.*
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
    FROM site_codes_to_source_concept_mapping 
    LEFT JOIN `ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71` cr 
        ON site_codes_to_source_concept_mapping.source_concept_id = cr.concept_id_1
        AND cr.invalid_reason IS NULL 
        AND lower(cr.relationship_id) = 'maps to'
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c2
        ON cr.concept_id_2 = c2.concept_id
        AND c2.invalid_reason IS NULL -- invalid records will map to concept_id = 0
)

SELECT DISTINCT * FROM target_concept_mapping
