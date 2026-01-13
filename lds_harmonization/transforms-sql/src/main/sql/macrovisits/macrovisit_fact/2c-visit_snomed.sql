
--aggregate binary proc-visit flags up up to the visit level
CREATE TABLE `ri.foundry.main.dataset.06d970fb-12d2-437b-87a3-688089edd4b5` AS
SELECT distinct visit_occurrence_id, 
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
                max(sno_office) as sno_office
FROM `ri.foundry.main.dataset.5f0e4d63-0d16-461d-a3e0-57695ab02d8e`
group by visit_occurrence_id