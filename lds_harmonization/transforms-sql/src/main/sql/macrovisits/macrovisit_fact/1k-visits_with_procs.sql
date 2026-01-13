--identify visits that procedures (HCPC or sno-med) that clearly indicate visits to office, ER, hosp-OBS, hosp-inpt, hosp-icu
CREATE TABLE `ri.foundry.main.dataset.5f0e4d63-0d16-461d-a3e0-57695ab02d8e` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_LARGE, NUM_EXECUTORS_16,EXECUTOR_MEMORY_OVERHEAD_EXTRA_LARGE') AS
select a.*,
        
        --case when hcpc in ('99291','99292') then 1 else 0 end as hcpc_icu, 
       --case when hcpc in ('99281','99282','99283','99284','99285') then 1 else 0 end as hcpc_er,
       --case when hcpc in ('99217','99218','99219','99220') then 1 else 0 end as hcpc_obs,
       --case when hcpc in ('99201','99202','99203','99204','99205','99211','99212','99213','99214','99215') then 1 else 0 end as hcpc_office,
       --case when hcpc in ('99221','99222','99223','99231','99232','99233','99234','99235','99236','99238','99239','99251','99253','99252','99254','99254') then 1 else 0 end as hcpc_inpt,

       case when procedure_concept_id in (2514441, 2514442) then 1 else 0 end as sno_icu,
       case when procedure_concept_id in (2514433, 2514434, 2514435, 2514436, 2514437) then 1 else 0 end as sno_er,
       case when procedure_concept_id in (2514400, 2514401, 2514402, 2514403) then 1 else 0 end as sno_obs,
       case when procedure_concept_id in (2414390, 2414391, 2414392, 2414393, 2414394, 2414395, 2414396, 2414397, 2414398, 2414395, 2514415, 2514416, 2514417, 2514418, 2514419) then 1 else 0 end as sno_office,
       case when procedure_concept_id in (2514404, 2514405, 2514406, 2514407, 2514408, 2514409, 2514413, 2514414, 2514420, 2514421, 2514422, 2514423, 2514424) then 1 else 0 end as sno_inpt
        from (
            
SELECT a.visit_occurrence_id, 
        a.visit_start_date, 
        a.visit_end_date,
        b.procedure_concept_id,
       b.procedure_source_value,
      -- substring(replace(procedure_source_value, 'CPT:'), 1,5) as hcpc,
      --regexp_extract(replace(replace(procedure_source_value,'CPT:'),'CPT4:',''), '(^[A-Z][0-9]{1,5}$|^[0-9]{5}$)') as hcpc,
       b.procedure_concept_name
FROM `ri.foundry.main.dataset.a956594c-7d67-488a-98b5-fb67b899c824` a INNER JOIN `ri.foundry.main.dataset.7394de3a-f3fe-4b73-9e61-12858dcd9868` b on a.visit_occurrence_id = b.visit_occurrence_id

       ) a

