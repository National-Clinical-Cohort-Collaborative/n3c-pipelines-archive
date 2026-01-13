CREATE TABLE `ri.foundry.main.dataset.cd7d81b2-7251-4f80-bc2b-ed78305fd394` AS

    SELECT 
            * 
    FROM (
        SELECT
            person_id as site_person_id
            , death_date	
            , death_datetime	
            , death_type_concept_id	
            , cause_concept_id	
            , cause_source_value	
            , cause_source_concept_id	
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.1de9a49e-4959-48cb-9b45-8309c714401d`
    ) 
