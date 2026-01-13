CREATE TABLE `ri.foundry.main.dataset.e134b541-36c3-487c-8454-a22a97c05be0` AS

SELECT 
de.data_partner_id,
mn.cdm_name,
device_concept_id, 
device_concept_name, count(*) AS num_rows
FROM `ri.foundry.main.dataset.dc046eb3-787a-41fd-b096-15f65eef3c5b` de
left join `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` mn
on de.data_partner_id = mn.data_partner_id
left join `ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6` csm
on de.device_concept_id = csm.concept_id
WHERE 
csm.concept_set_name = 'O2 device' and csm.is_most_recent_version is true
group by  
de.data_partner_id,
mn.cdm_name,
device_concept_id, 
device_concept_name


