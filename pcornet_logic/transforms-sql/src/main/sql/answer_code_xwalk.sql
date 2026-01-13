CREATE TABLE `ri.foundry.main.dataset.9c88070e-c967-4662-a45b-73bd72b579b1` AS
    -- PCORNet sites are sending in SDoH concepts via the obs_gen
    -- With the SDoH data, the categorical answer texts are coming in as the LOINC answer codes in the obsgen_result_text. 
    -- Here we are generating a crosswalk lookup table to translate all possible answer codes and the target_concept_id to retrieve the valid value_as_concept_ids.
    SELECT DISTINCT
    'OBS_GEN' AS CDM_TBL,
    obs.obsgen_result_text as answer_code,
    c.concept_code, 
    obs.obsgen_type as src_vocab_code,
    c.vocabulary_id,
    COALESCE( c.concept_id, 0) as target_concept_id -- if no mapping is found set it to 0
    FROM `ri.foundry.main.dataset.b52c4215-d873-4931-a8a8-455282ec9a06` obs
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON trim(c.concept_code) = trim(obs.obsgen_result_text) 
            AND upper(c.vocabulary_id) = 'LOINC'
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    WHERE obs.obsgen_type = 'LC' and obs.obsgen_result_modifier= 'TX' and obs.obsgen_result_text is not null  ----LC types are SDoH LOINC coded concept types