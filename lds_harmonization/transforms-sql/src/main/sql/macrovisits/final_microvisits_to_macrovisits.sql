
--original program (below) fully deprecated to facilitate using macrovisit_fact as a source for final table

CREATE TABLE `ri.foundry.main.dataset.de27cd23-ea8f-4d9f-825e-40b61cd2c1e2` TBLPROPERTIES (foundry_transform_profiles = 'DRIVER_MEMORY_LARGE, EXECUTOR_MEMORY_MEDIUM, NUM_EXECUTORS_16') AS
with macrovisit_fact_details as (
    SELECT
        macrovisit_id,
        covid_dx,
        all_icu,
        likely_hospitalization
    FROM `ri.foundry.main.dataset.d1768ebe-0659-4920-b8ca-ee0940aeed5a`
    WHERE
        macrovisit_id is not null
)

select /*+ BROADCAST(macrovisit_fact_details) */  
    processed_microvisits_to_macrovisits.*, 
    macrovisit_fact_details.covid_dx,
    macrovisit_fact_details.all_icu,
    macrovisit_fact_details.likely_hospitalization

from `ri.foundry.main.dataset.3ba38c5f-be15-49d6-9fcb-5ee93230503d` processed_microvisits_to_macrovisits
LEFT JOIN macrovisit_fact_details 
    on processed_microvisits_to_macrovisits.macrovisit_id = macrovisit_fact_details.macrovisit_id
    

    
--CREATE TABLE `ri.foundry.main.dataset.de27cd23-ea8f-4d9f-825e-40b61cd2c1e2` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_LARGE, NUM_EXECUTORS_16,EXECUTOR_MEMORY_OVERHEAD_EXTRA_LARGE') AS
    
    -- This final cleanup step removes macrocvisits that consist ONLY of NON-IP visits
    -- We firstbuild a list of all macrovisits that have at least one IP or ER visit
    -- Then keep all data on those macrovisits, and discard the rest

--PJL & AG deprecated the first 2 steps of this program 7/28/2022 based on overall revisions to macrovisit process
-- Find the distinct macrovisit_ids that contain at least one IP (micro)visit
--WITH ip_and_er_microvisits AS
--(
--    SELECT DISTINCT macrovisit_id
--    FROM `ri.foundry.main.dataset.7ac2fb3e-c124-4ed8-a384-0f5deb49e949`
--    WHERE visit_concept_id IN (9201, 8717, 262, 32037, 581379, 581385, 8756, 9203) --PJL removed 9203 6/17/2022
--),

-- Filter the macrovisit table to only those macrovisits that contain at least one IP visit
--macrovisits_containing_at_least_one_ip_visit AS
--(
--    SELECT m.* 
--    FROM `ri.foundry.main.dataset.7ac2fb3e-c124-4ed8-a384-0f5deb49e949` m
--    INNER JOIN ip_and_er_microvisits i
--    ON m.macrovisit_id = i.macrovisit_id
--)

-- Build a copy of the visit table with macrovisit_id added
-- leaving macrovisit_id null if the initial macrovisit contained no IP visits of any type
--SELECT  v.*,
--        m.macrovisit_id,
--        m.macrovisit_start_date,
--        m.macrovisit_end_date
--FROM `ri.foundry.main.dataset.a956594c-7d67-488a-98b5-fb67b899c824` v
--LEFT JOIN  `ri.foundry.main.dataset.7ac2fb3e-c124-4ed8-a384-0f5deb49e949` m
--ON v.visit_occurrence_id = m.visit_occurrence_id


