CREATE TABLE `ri.foundry.main.dataset.676c56e7-474d-4b2a-9da6-50194eb1a0b9` AS
with most_recent_version_concept_set_members as (
    SELECT
        concept_id,
        concept_set_name
    from `ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6` 
    where
        is_most_recent_version = 'true'  
        and
        concept_set_name in (
            'sdoh_questions'
            ,'sdoh_answers'
        )
)
select data_partner_id, 
       cdm_name,
       observation_concept_id, 
       observation_concept_name, 
       sdoh_category,
       count(distinct observation_id) as row_count
from (
    SELECT /*+ BROADCAST(csm, mn) */ 
    ob.data_partner_id, 
    mn.cdm_name,
    observation_concept_id, 
    observation_concept_name, 
    csm.concept_set_name as sdoh_category,
    observation_id
    FROM `ri.foundry.main.dataset.f925d142-3b25-498e-8538-4b9685c5270f` ob
    inner join most_recent_version_concept_set_members csm
    on ob.observation_concept_id = csm.concept_id
    left join `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` mn
    on ob.data_partner_id = mn.data_partner_id

    union all

    SELECT /*+ BROADCAST(csm, mn) */ 
    ob.data_partner_id, 
    mn.cdm_name,
    ob.value_as_concept_id as observation_concept_id, 
    ob.value_as_concept_name as observation_concept_name, 
    csm.concept_set_name as sdoh_category,
    observation_id
    FROM `ri.foundry.main.dataset.f925d142-3b25-498e-8538-4b9685c5270f` ob
    inner join most_recent_version_concept_set_members csm
    on ob.value_as_concept_id = csm.concept_id --Bring in SDoH answers that are present in the value_as_concept_id column.
    left join `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` mn
    on ob.data_partner_id = mn.data_partner_id
)
group by 
data_partner_id, 
       cdm_name,
       observation_concept_id, 
       observation_concept_name, 
       sdoh_category