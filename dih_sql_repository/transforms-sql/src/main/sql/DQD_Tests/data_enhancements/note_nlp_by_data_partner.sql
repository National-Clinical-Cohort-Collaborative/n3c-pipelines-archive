CREATE TABLE `ri.foundry.main.dataset.c830c4b2-8c53-431f-ba06-64b062d586d4` AS
    SELECT x.data_partner_id, 
    x.cdm_name,
    sum(x.non_null_concepts) as mapped_concept_count,
    count(x.non_null_concepts) as total_concept_count
    FROM (    
    SELECT 
    nlp.data_partner_id,
    mn.cdm_name,
    case when nlp.note_nlp_concept_name is null then 0 else 1 end as non_null_concepts,
    nlp.note_nlp_concept_name
    FROM `ri.foundry.main.dataset.ac5646c7-1399-4e0c-a8de-be1fa660ae90` nlp
    left join `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` mn
    on nlp.data_partner_id = mn.data_partner_id
    ) x
    group by
    x.data_partner_id, 
    x.cdm_name 
