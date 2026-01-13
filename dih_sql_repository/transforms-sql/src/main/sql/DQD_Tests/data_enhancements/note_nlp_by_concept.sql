CREATE TABLE `ri.foundry.main.dataset.4caa7e9e-60dc-46e2-8717-38ab177cb0be` AS
    with hpo_concepts as (
     SELECT id, name, 
     replace(id, 'HP:', '199') as note_nlp_concept_id 
     FROM `ri.foundry.main.dataset.9b498106-21ba-466e-a739-e6e79259ab79`
     where id like 'HP:%'
    )   
    SELECT x.data_partner_id,
    x.cdm_name,
    x.note_nlp_concept_name,
    count(*) as note_nlp_concept_name_count
    FROM (
    SELECT nlp.data_partner_id,
    mn.cdm_name, 
    coalesce(hpo.name, nlp.note_nlp_concept_name, nlp.note_nlp_source_concept_name) as note_nlp_concept_name
    FROM `ri.foundry.main.dataset.ac5646c7-1399-4e0c-a8de-be1fa660ae90` nlp
    left join `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` mn
    on nlp.data_partner_id = mn.data_partner_id
    left join hpo_concepts hpo
    on nlp.note_nlp_concept_id = hpo.note_nlp_concept_id
    ) x
    group by
    x.data_partner_id,
    x.cdm_name,
    x.note_nlp_concept_name
