--join macrovisits to visit-level counts of resources
--this creates duplicates of macrovists that will be subsequently de-duplicated
CREATE TABLE `ri.foundry.main.dataset.2e21febe-b3ae-4fbf-9d47-ed303894b78b` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_LARGE, NUM_EXECUTORS_16,EXECUTOR_MEMORY_OVERHEAD_EXTRA_LARGE') AS
SELECT distinct a.*, 
                b.dx_count, 
                c.drug_count, 
                d.proc_count, 
                e.meas_count, 
                f.obs_count, 
                g.covid_dx,
                --h.hcpc_anes, 
                --h.hcpc_er, 
                --h.hcpc_icu, 
                --h.hcpc_inpt, 
                --h.hcpc_obs, 
                --h.hcpc_office,
                h.sno_er,
                h.sno_icu,
                h.sno_inpt,
                h.sno_obs,
                h.sno_office,
                i.drg, 
                i.value_as_concept_name,
                j.cms_inpt, 
                k.max_cms_inpt,
                k.sum_cms_inpt,
                k.max_hcpc_office,
                k.sum_hcpc_office,
                k.max_hcpc_er,
                k.sum_hcpc_er,
                k.max_hcpc_obs,
                k.sum_hcpc_obs,
                k.max_hcpc_inpt,
                k.sum_hcpc_inpt,
                k.max_hcpc_icu,
                k.sum_hcpc_icu
 --case when lower(visit_concept_name) like ('%inpa%') then 1 end as inpt_visit,

FROM `ri.foundry.main.dataset.3ba38c5f-be15-49d6-9fcb-5ee93230503d` a LEFT JOIN `ri.foundry.main.dataset.b55b2e1f-c2aa-40e8-8dd6-92ac6f2f2888` b on a.visit_occurrence_id = b.visit_occurrence_id
                                    LEFT JOIN `ri.foundry.main.dataset.256f2058-90ba-4ecb-ad26-8267ce5cff59` c on a.visit_occurrence_id = c.visit_occurrence_id
                                    LEFT JOIN `ri.foundry.main.dataset.32704111-069c-4f5a-8081-4e492b8ac36b` d on a.visit_occurrence_id = d.visit_occurrence_id
                                    LEFT JOIN `ri.foundry.main.dataset.d3fe9c88-f795-418f-bab2-f69db5bdb5b1` e on a.visit_occurrence_id = e.visit_occurrence_id
                                    LEFT JOIN `ri.foundry.main.dataset.9d595e5a-a0ad-4681-ace8-4198fab85943` f on a.visit_occurrence_id = f.visit_occurrence_id
                                    LEFT JOIN `ri.foundry.main.dataset.959048f1-4946-4acc-9bf9-7bee79c54fc1` g on a.visit_occurrence_id = g.visit_occurrence_id
                                    LEFT JOIN `ri.foundry.main.dataset.06d970fb-12d2-437b-87a3-688089edd4b5` h on a.visit_occurrence_id = h.visit_occurrence_id
                                    LEFT JOIN `ri.foundry.main.dataset.5b45767e-fa7e-43db-bc5f-001195db02ce` i on a.visit_occurrence_id = i.visit_occurrence_id
                                    LEFT JOIN `ri.foundry.main.dataset.bc03f2f1-f2fc-4abe-8353-28f9f0409614` j on a.visit_occurrence_id = j.visit_occurrence_id
                                    LEFT JOIN `ri.foundry.main.dataset.96a926c9-83ac-4662-a048-ac3a6d36c394` k on a.visit_occurrence_id = k.visit_occurrence_id
where macrovisit_id is not null
order by person_id, visit_start_date