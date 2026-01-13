CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/bad_dates_start_end` TBLPROPERTIES (foundry_transform_profiles = 'NUM_EXECUTORS_16, EXECUTOR_MEMORY_SMALL') AS
-- Counts dates that are before 1/1/2000 or after the last_plausible_date in the Manifest.
-- Breaks them out by data_partner_id, domain, too early vs too late, and start vs end.
-- This script and table only do so for table that have a pair of dates.
-- manifest_clean_with_last_plausible_date is created from manifest_clean by last_plausible_date.py in DI&H Python Repo
WITH  MANI as (
    select data_partner_id, last_plausible_date, to_date('2000-01-01', 'yyyy-MM-dd') as too_early 
    from `ri.foundry.main.dataset.1e1f4870-a1ef-47d8-8f98-d352a3e22368`
)
select 'condition_occurrence start' as domain, c.data_partner_id,
    count(*) as ct,
    count(case  when condition_start_date is null then 1 end) as is_null,
    count(case  when datediff(condition_start_date, MANI.too_early) < 0 then 1 end) as early,
    count(case  when datediff(condition_start_date, MANI.last_plausible_date) > 0 then 1 end) as future
    FROM `ri.foundry.main.dataset.641c645d-eacf-4a5a-983d-3296e1641f0c` c
    JOIN MANI on MANI.data_partner_id = c.data_partner_id
    group by domain, c.data_partner_id
union all
    select 'condition_occurrence end' as domain, c.data_partner_id,
    count(*) as ct,
    count(case  when condition_end_date is null then 1 end) as is_null,
    count(case  when datediff(condition_end_date, MANI.too_early) < 0 then 1 end) as early,
    count(case  when datediff(condition_end_date, MANI.last_plausible_date)  > 0 then 1 end) as future
    FROM `ri.foundry.main.dataset.641c645d-eacf-4a5a-983d-3296e1641f0c` c
    JOIN MANI on MANI.data_partner_id = c.data_partner_id
    group by domain, c.data_partner_id
union all    
    select 'drug_exposure start' as domain, de.data_partner_id,
    count(*) as ct,
    count(case  when drug_exposure_start_date is null then 1 end) as is_null,
    count(case  when datediff(drug_exposure_start_date, MANI.too_early) < 0 then 1 end) as early, 
    count(case  when datediff(drug_exposure_start_date, MANI.last_plausible_date) > 0 then 1 end) as future
    FROM `ri.foundry.main.dataset.3feda4dc-5389-4521-ab9e-0361ea5773fd` de
    JOIN MANI on MANI.data_partner_id = de.data_partner_id
    group by domain, de.data_partner_id
union all    
    select 'drug_exposure end' as domain, de.data_partner_id,
    count(*) as ct,
    count(case  when drug_exposure_end_date is null then 1 end) as is_null,
    count(case  when datediff(drug_exposure_end_date, MANI.too_early) < 0 then 1 end) as early,
    count(case  when datediff(drug_exposure_end_date, MANI.last_plausible_date)  > 0 then 1 end) as future
    FROM `ri.foundry.main.dataset.3feda4dc-5389-4521-ab9e-0361ea5773fd` de
    JOIN MANI on MANI.data_partner_id = de.data_partner_id
    group by domain, de.data_partner_id
union all 
    select 'visit_occurrence start' as domain, vo.data_partner_id,
    count(*) as ct,
    count(case  when visit_start_date is null then 1 end) as is_null,
    count(case  when datediff(visit_start_date, MANI.too_early) < 0 then 1 end) as early,
    count(case  when datediff(visit_start_date, MANI.last_plausible_date) > 0 then 1 end) as future
    FROM `ri.foundry.main.dataset.749e3f71-9b0e-4896-af8b-91aefe71b308` vo
    JOIN MANI on MANI.data_partner_id = vo.data_partner_id
    group by domain, vo.data_partner_id
union all 
    select 'visit_occurrence end' as domain, vo.data_partner_id,
    count(*) as ct,
    count(case  when visit_end_date is null then 1 end) as is_null,
    count(case  when datediff(visit_end_date, MANI.too_early) < 0 then 1 end) as early,
    count(case  when datediff(visit_end_date, MANI.last_plausible_date)  > 0 then 1 end) as  future
    FROM `ri.foundry.main.dataset.749e3f71-9b0e-4896-af8b-91aefe71b308` vo
    JOIN MANI on MANI.data_partner_id = vo.data_partner_id
    group by domain, vo.data_partner_id


