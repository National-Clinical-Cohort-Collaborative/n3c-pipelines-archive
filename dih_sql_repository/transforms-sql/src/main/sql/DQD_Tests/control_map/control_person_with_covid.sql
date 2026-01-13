CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/control_map/control_person_with_covid` AS
    with controlmap as (

        SELECT control_map_id, case_person_id, buddy_num, control_person_id, data_partner_id
        FROM `ri.foundry.main.dataset.4375ef4b-9630-4682-9634-3df04e5b099f`
    ),

    --case person is valid n3c person
    n3c_case_person_control_person as (
    SELECT p.person_id as case_person_id, p.data_partner_id, cm.control_person_id as cm_control_person_id
    FROM `ri.foundry.main.dataset.456df227-b15d-4356-9df6-b113e90eb239` p
    inner join controlmap cm on p.person_id =cm.case_person_id and p.data_partner_id = cm.data_partner_id
    ),

    covid_test as (

    SELECT concept_id
    FROM `/N3C Export Area/Concept Set Ontology/Concept Set Ontology/hubble_base/concept_set_members` cs
    WHERE cs.concept_set_name ='ATLAS SARS-CoV-2 rt-PCR and AG'
            AND cs.is_most_recent_version = True
    ),

    covidpat_test_confirmed_pos as (
    SELECT  m.person_id, m.data_partner_id, m.measurement_concept_id, m.value_as_concept_id
    FROM `ri.foundry.main.dataset.1937488e-71c5-4808-bee9-0f71fd1284ac` m
    where m.measurement_concept_id in (select concept_id from covid_test) 
    and m.value_as_concept_id in 
        ( SELECT concept_id 
             FROM `/N3C Export Area/Concept Set Ontology/Concept Set Ontology/hubble_base/concept_set_members` cs
            WHERE cs.concept_set_name in ('ResultPos')
            AND cs.is_most_recent_version = True
        )
    ),

    coviddx as (
        SELECT concept_id
        FROM `/N3C Export Area/Concept Set Ontology/Concept Set Ontology/hubble_base/concept_set_members` cs
        WHERE cs.concept_set_name in ('[DM]COVID-19 diagnosis record or SARS-CoV-2 lab confirmed positive')
        AND cs.is_most_recent_version = True
    ),

    covidpat_dx as (
    SELECT distinct person_id, data_partner_id, c.condition_concept_id
       FROM `ri.foundry.main.dataset.641c645d-eacf-4a5a-983d-3296e1641f0c` c
    where c.condition_concept_id in (select concept_id from coviddx)        
    ),

    covidpat_dx_and_test_confirmed as (
        SELECT m.person_id, m.measurement_concept_id as concept_id, m.data_partner_id
        FROM covidpat_test_confirmed_pos m
        union
        select c.person_id, c.condition_concept_id as concept_id, c.data_partner_id
        from covidpat_dx c
    )

--- cm_control_person_id who is found to be covid positive patient in n3c 
--- for every case person site are submitting two control person and control person cannot be covid_positive patient 
---
SELECT cc.person_id, cc.concept_id, cc.data_partner_id
FROM covidpat_dx_and_test_confirmed cc
join n3c_case_person_control_person ccp on ccp.cm_control_person_id = cc.person_id and ccp.data_partner_id = cc.data_partner_id
