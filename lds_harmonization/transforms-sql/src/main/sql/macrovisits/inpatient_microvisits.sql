-- Selects any inpatient (IP) visit by the codes below, 
-- Any OP visit that is exactly 1 day --deprecated
-- Any ER visit that is >= 1 day  --updated to >=2 days
--  These are assumed to be IP visits.
-- These visits will be assembled into the scaffold of the macrovisit.

    -- 262	Emergency Room and Inpatient Visit	
    -- 8717	Inpatient Hospital
    -- 9201	Inpatient Visit		
    -- 32037	Intensive Care	
    -- 581379	Inpatient Critical Care Facility	

    -- inpatient visit, inpatient hospital, emergency room and intensive care

    -- non-inpatient visits that are also referenced
    -- 9203 Emergency room
    -- 9202 Outpatient visit
    -- 581385 Observation room
    -- 8676 Nursing facility
    -- 8756 Outpatient hospital

CREATE TABLE `ri.foundry.main.dataset.91d360c3-986c-4c0c-b7a5-9020567ad045` AS
    

    SELECT *     

    FROM `ri.foundry.main.dataset.a956594c-7d67-488a-98b5-fb67b899c824`

    WHERE  
    
    -- we only use visits with defined start and end dates in the scaffold
    -- visits with a start date but lacking an end date can still be joined in with the OTHER visits
    -- at the end

    (visit_start_date IS NOT NULL AND visit_end_date IS NOT NULL)

    -- Doc Brown warned us never to time travel
    AND visit_start_date <= visit_end_date
    
    AND

    -- Visit Types that are allowed to be part of the macrovisit scaffold
    (
        -- Any inpatient visit
        visit_concept_id in (9201, 8717, 262, 32037, 581379)
        
      --  OR

        -- A one day Outpatient visit 
        -- It was decided that a 1-day OP visit could be an Overnight procedure
        -- But the very long OP visits are likely an administrative concept and don't reflect actual hospital stays
        -- PJL deprecated this inclusion 6/17/2022
       -- (visit_concept_id = 9202 AND DATEDIFF(visit_end_date,visit_start_date) = 1)

        OR

        -- ER visit >= 2 days
        -- updated from >=1 day to >=2 days PJL 6/19/2022
        (visit_concept_id = 9203 AND DATEDIFF(visit_end_date,visit_start_date) >= 2)

        OR
        --Observation room >=2 days
        (visit_concept_id = 581385 AND DATEDIFF(visit_end_date,visit_start_date) >= 2)

        OR
        --Outpatient Hospital >=2 days
        (visit_concept_id = 8756 AND DATEDIFF(visit_end_date,visit_start_date) >= 2)
    )


