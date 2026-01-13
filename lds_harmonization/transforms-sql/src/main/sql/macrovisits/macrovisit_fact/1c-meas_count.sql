
    --count measures for each visit
CREATE TABLE `ri.foundry.main.dataset.d3fe9c88-f795-418f-bab2-f69db5bdb5b1` TBLPROPERTIES (foundry_transform_profiles = 'NUM_EXECUTORS_16, SHUFFLE_PARTITIONS_LARGE') AS
SELECT visit_occurrence_id, count(measurement_id) as meas_count
FROM `ri.foundry.main.dataset.0f30f7f8-dc17-4f19-b9a7-1297708050ff`
where visit_occurrence_id is not null
group by visit_occurrence_id