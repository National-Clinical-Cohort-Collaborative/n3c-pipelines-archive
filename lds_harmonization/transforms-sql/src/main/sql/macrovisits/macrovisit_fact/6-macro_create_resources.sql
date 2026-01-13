--deduplicate last node by taking just the max resources for each macrovisit-visit combination


CREATE TABLE `ri.foundry.main.dataset.cbab7afa-d662-4c41-84b8-97c5e78277a2` AS

select distinct a.macrovisit_id, person_id, data_partner_id, datediff(macrovisit_end_date, macrovisit_start_date) as los,
    max(resources) as max_resources,
    max(covid_dx) as covid_dx,
    --max(hcpc_anes) as hcpc_anes,
    --max(hcpc_er) as hcpc_er,
    --max(hcpc_icu) as hcpc_icu,
    --max(hcpc_inpt) as hcpc_inpt,
    --max(hcpc_obs) as hcpc_obs,
    --max(hcpc_office) as hcpc_office,
    max(sno_er) as sno_er,
    max(sno_icu) as sno_icu,
    max(sno_inpt) as sno_inpt,
    max(sno_obs) as sno_obs,
    max(sno_office) as sno_office,
    max(drg) as drg,
    max(dx_count) as max_dx_count,
    max(proc_count) as max_proc_count,
    max(drug_count) as max_drug_count,
    max(meas_count) as max_meas_count,
    max(obs_count) as max_obs_count,
    max(cms_inpt) as cms_inpt_old,
    max(max_cms_inpt) as max_cms_inpt,
    max(sum_cms_inpt) as max_sum_cms_inpt,
    max(max_hcpc_office) as max_hcpc_office,
    max(sum_hcpc_office) as max_sum_hcpc_office,
    max(max_hcpc_er) as max_hcpc_er,
    max(sum_hcpc_er) as max_sum_hcpc_er,
    max(max_hcpc_obs) as max_hcpc_obs,
    max(sum_hcpc_obs) as max_sum_hcpc_obs,
    max(max_hcpc_inpt) as max_hcpc_inpt,
    max(sum_hcpc_inpt) as max_sum_hcpc_inpt,
    max(max_hcpc_icu) as max_hcpc_icu,
    max(sum_hcpc_icu) as max_sum_hcpc_icu
      from (


SELECT nvl(dx_count,0)+nvl(proc_count,0)+nvl(drug_count,0)+nvl(meas_count,0) as resources, * FROM `ri.foundry.main.dataset.2e21febe-b3ae-4fbf-9d47-ed303894b78b`
) a 
group by a.macrovisit_id, person_id, data_partner_id, datediff(macrovisit_end_date, macrovisit_start_date)