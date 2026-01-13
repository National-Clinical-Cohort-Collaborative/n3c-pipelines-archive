CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/omop-non-standard-concepts/non_standard_concepts_found_in_omop_domain` AS
    
    ---generate stats of non-standard concepts found in omop cdm submission
    SELECT 
    dom.domain
    , dom.concept_id as omop_domain_concept_id
    , dom.total_count
    , dom.distinct_person_count
    , dom.data_partner_id
    , c.*
    FROM `ri.foundry.main.dataset.0f440603-9d5b-43e9-adbf-f3f743f45ad7` dom
    JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    on c.concept_id = dom.concept_id 
    --filtered to omop sites only
    WHERE (c.standard_concept IS NULL OR c.standard_concept != 'S' ) and dom.data_partner_id in ( 
      SELECT data_partner_id from `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683`
      WHERE cdm_name = 'OMOP'
    )
    