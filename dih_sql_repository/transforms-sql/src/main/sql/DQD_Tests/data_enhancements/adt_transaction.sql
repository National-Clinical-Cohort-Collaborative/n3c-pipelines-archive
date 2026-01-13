CREATE TABLE `ri.foundry.main.dataset.058a412e-7d7f-4829-8097-afe06bd94808` AS


select data_partner_id, 
cdm_name,
sum(emergency) as emergency_visits,
sum(inpatient) as inpatient_visits,
sum(icu) as icu_visits,
sum(no_matching_concept) as no_matching_concept_visits,
sum(other_concepts) as other_concepts_visits,
count(*) as total_visits,
sum(null_end_dates) as null_end_date_visits,
round((sum(null_end_dates)/count(*))*100, 1) as null_end_date_perc
from (
    SELECT
    vd.data_partner_id, 
    mn.cdm_name,
    case when visit_detail_end_date is null then 1 else 0 end as null_end_dates,
    case when visit_detail_concept_id = 8870 then 1 else 0 end as emergency,
    case when visit_detail_concept_id = 8717 then 1 else 0 end as inpatient,
    case when visit_detail_concept_id = 581379 then 1 else 0 end as icu,
    case when visit_detail_concept_id = 0 then 1 else 0 end as no_matching_concept,
    case when visit_detail_concept_id in (8870, 8717, 581379, 0) then 0 else 1 end as other_concepts
FROM `ri.foundry.main.dataset.46b0c24e-b71c-49a1-9f70-0b163315bb09` vd
left join `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` mn
on vd.data_partner_id = mn.data_partner_id
) group by data_partner_id, cdm_name
