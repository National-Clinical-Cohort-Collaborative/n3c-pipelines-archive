
--count diangoses for each visit
CREATE TABLE `ri.foundry.main.dataset.b55b2e1f-c2aa-40e8-8dd6-92ac6f2f2888` TBLPROPERTIES (foundry_transform_profiles = 'SHUFFLE_PARTITIONS_LARGE') AS
SELECT visit_occurrence_id,
 person_id, 
 count(condition_occurrence_id) as dx_count,
 min(condition_start_date) as min_dx_date,
 max(condition_start_date) as max_dx_date
FROM `ri.foundry.main.dataset.17e798ef-2459-4d3a-98ef-ac515b83871a`
group by visit_occurrence_id, person_id