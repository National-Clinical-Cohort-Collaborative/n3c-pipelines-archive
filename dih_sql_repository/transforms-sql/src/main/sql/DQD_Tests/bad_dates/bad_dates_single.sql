CREATE TABLE `ri.foundry.main.dataset.048f3439-0417-48bb-b7f1-b433b54cef5b` TBLPROPERTIES (foundry_transform_profiles = 'NUM_EXECUTORS_16, EXECUTOR_MEMORY_SMALL') AS
-- Counts dates that are before 1/1/2000 or after the last_plausible_date in the Manifest.
-- Breaks them out by data_partner_id, domain, too early vs too late.
-- This script and table only do so for table that have a single date.
-- manifest_clean_with_last_plausible_date is created from manifest_clean by last_plausible_date.py in DI&H Python Repo
WITH  MANI as (
    select data_partner_id, last_plausible_date, to_date('2000-01-01', 'yyyy-MM-dd') as too_early 
    from `ri.foundry.main.dataset.1e1f4870-a1ef-47d8-8f98-d352a3e22368`
)
     select "measurement" as domain, m.data_partner_id,  
     count(*) as ct,
     count(case when measurement_date is null then 1 end) as is_null,
     count(case when datediff(measurement_date, MANI.too_early) < 0 then 1 end) as early,
     count(case when datediff(measurement_date, MANI.last_plausible_date) > 0 then 1  end) as future
     FROM `ri.foundry.main.dataset.1937488e-71c5-4808-bee9-0f71fd1284ac` m
     JOIN MANI on MANI.data_partner_id = m.data_partner_id
     group by domain, m.data_partner_id
union all     
     select 'observation' as domain, o.data_partner_id,
     count(*) as ct,
     count(case when observation_date is null then 1 end) as is_null,
     count(case when datediff(observation_date, MANI.too_early) < 0 then 1 end) as early,
     count(case when datediff(observation_date, MANI.last_plausible_date) > 0 then 1 end) as future
     FROM `ri.foundry.main.dataset.f925d142-3b25-498e-8538-4b9685c5270f` o
     JOIN MANI on MANI.data_partner_id = o.data_partner_id
     group by domain, o.data_partner_id
union all    
     select 'procedure_occurrence' as domain, po.data_partner_id, 
     count(*) as ct,
     count(case when procedure_date is null then 1 end) as is_null,
     count(case when datediff(procedure_date, MANI.too_early) < 0 then 1 end) as early,
     count(case when datediff(procedure_date, MANI.last_plausible_date) > 0 then 1 end) as future
     FROM `ri.foundry.main.dataset.2a3c8355-7fbf-478f-bbbf-eabb6733b04d` po
     JOIN MANI on MANI.data_partner_id = po.data_partner_id
     group by domain, po.data_partner_id
union all     
    select 'death' as domain, d.data_partner_id, 
    count(*) as ct,
    count(case when death_date is null then 1 end) as is_null,
    count(case when datediff(death_date, MANI.too_early) < 0 then 1 end) as early,  
    count(case when datediff(death_date, MANI.last_plausible_date) > 0 then 1 end ) as future 
    from  `ri.foundry.main.dataset.dfde51a2-d775-440a-b436-f1561d3f8e5d` d
    JOIN MANI on MANI.data_partner_id = d.data_partner_id
    group by domain, d.data_partner_id

