--count procedures on each visit
CREATE TABLE `ri.foundry.main.dataset.32704111-069c-4f5a-8081-4e492b8ac36b` AS
SELECT visit_occurrence_id, 
count(procedure_occurrence_id) as proc_count,
 min(procedure_date) as min_proc_date,
 max(procedure_date) as max_proc_date
FROM `ri.foundry.main.dataset.7394de3a-f3fe-4b73-9e61-12858dcd9868`
group by visit_occurrence_id