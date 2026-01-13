CREATE TABLE `ri.foundry.main.dataset.0270e7fe-4d0e-424e-866c-51cbda51f0ac` AS
    SELECT --distinct observation_concept_name
    ob.data_partner_id, 
    mn.cdm_name,
    observation_concept_id, 
    observation_concept_name, 
    count(*) as row_count 
    FROM `ri.foundry.main.dataset.f925d142-3b25-498e-8538-4b9685c5270f` ob
    left join `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` mn
    on ob.data_partner_id = mn.data_partner_id
    where observation_concept_id = '2004207791' or observation_source_concept_id = '2004207791' 
    group by ob.data_partner_id, 
    mn.cdm_name,
    observation_concept_id, 
    observation_concept_name