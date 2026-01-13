
CREATE TABLE `ri.foundry.main.dataset.89cf2b3a-9035-4803-8ca0-9d0ef5506d95` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_MEDIUM, NUM_EXECUTORS_8') AS
SELECT distinct
   data_partner_id,
    macrovisit_id,
    macrovisit_start_date,
    macrovisit_end_date, 
    count(visit_occurrence_id) as total_microvisits,
    datediff(macrovisit_end_date, macrovisit_start_date) as los,
    sum(case when visit_concept_name='Intensive Care' then 1 else 0 end) as intensive_care,
    sum(case when visit_concept_name='Inpatient Critical Care Facility' then 1 else 0 end) as inpt_icu,
    sum(case when visit_concept_name='Inpatient Hospital' then 1 else 0 end) as inpt_hospital,
    sum(case when visit_concept_name='Inpatient Visit' then 1 else 0 end) as inpt_visit,
    sum(case when visit_concept_name='Emergency Room and Inpatient Visit' then 1 else 0 end) as er_inpt,
    sum(case when visit_concept_name='Outpatient Visit' then 1 else 0 end) as outpt_visit,
    sum(case when visit_concept_name='Office Visit' then 1 else 0 end) as office_visit,
    sum(case when visit_concept_name='Emergency Room Visit' then 1 else 0 end) as er_visit,
    sum(case when visit_concept_name='Outpatient Hospital' then 1 else 0 end) as outpt_hosp,
    sum(case when visit_concept_name='Observation Room' then 1 else 0 end) as obs_room,
    sum(case when visit_concept_name='Laboratory Visit' then 1 else 0 end) as lab_visit,
    sum(case when visit_concept_name='Non-hospital institution Visit' then 1 else 0 end) as non_hosp,
    sum(case when visit_concept_name='Ambulatory Radiology Clinic/Center' then 1 else 0 end) as amb_rad,
    sum(case when visit_concept_name='Telehealth' then 1 else 0 end) as telehealth,
    sum(case when visit_concept_name='Pharmacy Visit' then 1 else 0 end) as pharmacy_visit,
    sum(case when visit_concept_name='Home Visit' then 1 else 0 end) as home_visit,
    sum(case when visit_concept_name='No matching concept' then 1 else 0 end) as no_matching,
    sum(case when visit_concept_name='No information' then 1 else 0 end) as no_info
 
FROM (
    select distinct * from `ri.foundry.main.dataset.3ba38c5f-be15-49d6-9fcb-5ee93230503d` where macrovisit_id is not null
) a
group by macrovisit_id, macrovisit_start_date, macrovisit_end_date, data_partner_id