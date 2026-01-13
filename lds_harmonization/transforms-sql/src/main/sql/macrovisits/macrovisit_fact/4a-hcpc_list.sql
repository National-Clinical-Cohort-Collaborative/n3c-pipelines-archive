
CREATE TABLE `ri.foundry.main.dataset.96a926c9-83ac-4662-a048-ac3a6d36c394` AS
SELECT visit_occurrence_id, 
    max(hcpc_office) as max_hcpc_office, 
    sum(hcpc_office) as sum_hcpc_office,
    max(hcpc_er) as max_hcpc_er,
    sum(hcpc_er) as sum_hcpc_er,
    max(hcpc_obs) as max_hcpc_obs,
    sum(hcpc_obs) as sum_hcpc_obs,
    max(hcpc_inpt) as max_hcpc_inpt,
    sum(hcpc_inpt) as sum_hcpc_inpt,
    max(hcpc_icu) as max_hcpc_icu,
    sum(hcpc_icu) as sum_hcpc_icu,
    max(cms_inpt_hcpc) as max_cms_inpt,
    sum(cms_inpt_hcpc) as sum_cms_inpt
FROM `ri.foundry.main.dataset.b8d5b872-f992-4531-8e70-403051b1c74f`
group by visit_occurrence_id