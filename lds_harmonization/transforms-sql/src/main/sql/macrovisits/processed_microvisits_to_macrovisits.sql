CREATE TABLE `ri.foundry.main.dataset.3ba38c5f-be15-49d6-9fcb-5ee93230503d` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_MEDIUM, NUM_EXECUTORS_8') AS

SELECT  v.*,
        m.macrovisit_id,
        m.macrovisit_start_date,
        m.macrovisit_end_date
FROM `ri.foundry.main.dataset.a956594c-7d67-488a-98b5-fb67b899c824` v
LEFT JOIN  `ri.foundry.main.dataset.7ac2fb3e-c124-4ed8-a384-0f5deb49e949` m
ON v.visit_occurrence_id = m.visit_occurrence_id
