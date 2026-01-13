
--counts of drugs on each visit
CREATE TABLE `ri.foundry.main.dataset.256f2058-90ba-4ecb-ad26-8267ce5cff59`  TBLPROPERTIES (foundry_transform_profiles = 'SHUFFLE_PARTITIONS_LARGE') AS
SELECT visit_occurrence_id, 
person_id,
 count(drug_exposure_id) as drug_count,
  min(drug_exposure_start_date) as min_drug_date,
   max(drug_exposure_start_date) as max_drug_date
FROM `ri.foundry.main.dataset.9c770c82-15d1-4d2f-888a-1f5082077997`
group by visit_occurrence_id, person_id