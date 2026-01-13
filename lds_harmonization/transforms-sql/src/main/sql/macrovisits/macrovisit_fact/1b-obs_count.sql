
 --count observations for each visit
CREATE TABLE `ri.foundry.main.dataset.9d595e5a-a0ad-4681-ace8-4198fab85943` TBLPROPERTIES (foundry_transform_profiles = 'SHUFFLE_PARTITIONS_LARGE') AS
SELECT visit_occurrence_id, count(observation_id) as obs_count
FROM `ri.foundry.main.dataset.1cb3fc9f-0fc6-4dbd-a1ad-da8bcac8da2a`
where visit_occurrence_id is not null
group by visit_occurrence_id
order by visit_occurrence_id