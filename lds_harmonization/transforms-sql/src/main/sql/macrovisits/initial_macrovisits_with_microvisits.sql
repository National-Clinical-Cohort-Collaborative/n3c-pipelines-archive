CREATE TABLE `ri.foundry.main.dataset.7ac2fb3e-c124-4ed8-a384-0f5deb49e949` TBLPROPERTIES (foundry_transform_profiles = 'DRIVER_MEMORY_MEDIUM, EXECUTOR_MEMORY_MEDIUM, NUM_EXECUTORS_8, AUTO_BROADCAST_JOIN_DISABLED') AS
with merging_intervals as (
    SELECT
        macrovisit_id,
        person_id,
        macrovisit_start_date,
        macrovisit_end_date
    from
        `ri.foundry.main.dataset.371672b0-9453-4fc1-b9b0-792f88e69983`
)
SELECT  
        v.person_id,
        v.data_partner_id,
        v.visit_occurrence_id,
        v.visit_concept_id,
        merging_intervals.macrovisit_id,
        merging_intervals.macrovisit_start_date,
        merging_intervals.macrovisit_end_date
FROM `ri.foundry.main.dataset.a956594c-7d67-488a-98b5-fb67b899c824` v
LEFT JOIN merging_intervals
ON v.person_id = merging_intervals.person_id
AND v.visit_start_date >= merging_intervals.macrovisit_start_date
AND v.visit_start_date <= merging_intervals.macrovisit_end_date 
