
--include many other metadata columns commented out in case there's a future need
CREATE TABLE `ri.foundry.main.dataset.d1768ebe-0659-4920-b8ca-ee0940aeed5a` AS
    SELECT distinct b.person_id,
        a.*,
        b.max_resources,
        b.covid_dx,
        --b.sno_er,
        --b.sno_icu,
        --b.sno_inpt,
        --b.sno_obs,
        --b.sno_office,
        --max_hcpc_office,
        --max_sum_hcpc_office,
        --max_hcpc_er,
        --max_sum_hcpc_er,
        --max_hcpc_obs,
        --max_sum_hcpc_obs,
        --max_hcpc_inpt,
        --max_sum_hcpc_inpt,
        --max_hcpc_icu,
        --max_sum_hcpc_icu
        b.drg,
        b.max_dx_count,
        b.max_proc_count,
        b.max_meas_count,
        b.max_obs_count,
        b.max_cms_inpt,
        b.max_sum_cms_inpt,
        c.all_office,
        c.all_er, 
        c.all_obs,
        c.all_inpt,
        c.all_icu,
        c.resources_bucket,
        d.likely_hospitalization
FROM `ri.foundry.main.dataset.280f65f2-9099-4e8f-b136-2c63871b526a` a LEFT JOIN `ri.foundry.main.dataset.cbab7afa-d662-4c41-84b8-97c5e78277a2` b on a.macrovisit_id = b.macrovisit_id
                        LEFT JOIN `ri.foundry.main.dataset.755f18d4-f44e-4dfe-b8ae-21f507f1bd74` c on a.macrovisit_id = c.macrovisit_id
                        LEFT JOIN `ri.foundry.main.dataset.56529a15-8d02-4ab9-82ae-ee67a1ce8730` d on a.macrovisit_id = d.macrovisit_id


