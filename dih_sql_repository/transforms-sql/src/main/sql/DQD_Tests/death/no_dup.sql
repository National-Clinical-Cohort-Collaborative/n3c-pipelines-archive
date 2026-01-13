CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/death/no_dup` AS


    with death as (
        SELECT   d.data_partner_id, d.death_date, d.person_id, date_part('YEAR', d.death_date) as year_of_death , p.year_of_birth                          
        FROM `ri.foundry.main.dataset.dfde51a2-d775-440a-b436-f1561d3f8e5d` d
        JOIN `ri.foundry.main.dataset.456df227-b15d-4356-9df6-b113e90eb239` p 
        ON d.person_id = p.person_id
        where d.death_date is not null
    ),

    covid_positive_A as (
        SELECT DISTINCT c.condition_concept_id as concept_id, c.data_partner_id, c.person_id, c.visit_occurrence_id
        FROM `/UNITE/LDS/clean/condition_occurrence` c
        WHERE c.condition_concept_id IN (4126681, 9191, 36032716, 45884084, 36715206, 45877985, 45878745, 45881802, 37311061) --concept_id of COVID positive in condition domain
        AND c.visit_occurrence_id IS NOT NULL
    ),

    covid_positive_B as (
        SELECT DISTINCT m.measurement_concept_id as concept_id, m.data_partner_id, m.person_id, m.visit_occurrence_id
        FROM `/UNITE/LDS/clean/measurement` m
        WHERE m.measurement_concept_id IN (SELECT DISTINCT concept_id 
            FROM `/N3C Export Area/Concept Set Ontology/Concept Set Ontology/hubble_base/concept_set_members` 
            WHERE codeset_id IN (386776576, 263281373) AND is_most_recent_version = true) 
            AND m.visit_occurrence_id IS NOT NULL
--measurement concepts from concept sets ATLAS SARS-CoV-2 rt-PCR and AG, Atlas #818 [N3C] CovidAntibody retry

    ),

    covid_positive as (
        SELECT a.*
        FROM covid_positive_A a
        FULL OUTER JOIN covid_positive_B b
        ON a.person_id = b.person_id AND a.data_partner_id = b.data_partner_id
    ),

    inpatient_admitted as (
        SELECT v.visit_occurrence_id, v.person_id, v.data_partner_id, v.visit_concept_id
        FROM `/UNITE/LDS/clean/visit_occurrence` v
        INNER JOIN `/UNITE/LDS/clean/person` p
        ON v.person_id = p.person_id AND v.data_partner_id = p.data_partner_id
        WHERE v.visit_concept_id IN (9201, 32037, 581379, 581383, 262, 8717)
    ),

    pos_inpatient as (
        --merge between positive patients and inpatient visit
        SELECT DISTINCT c.*, i.visit_concept_id
        FROM covid_positive c
        INNER JOIN inpatient_admitted i
        ON c.person_id = i.person_id AND c.data_partner_id = i.data_partner_id
    ),

    inpatient_cnt as (
        SELECT count(*) as total, data_partner_id
        FROM pos_inpatient
        GROUP BY data_partner_id
    ),

    death_inpatient as (
        SELECT DISTINCT p.concept_id, p.data_partner_id, p.person_id, p.visit_concept_id, d.death_date, d.year_of_death, d.year_of_birth
        FROM pos_inpatient p
        INNER JOIN death d
        ON p.person_id = d.person_id AND p.data_partner_id = d.data_partner_id
    )

    SELECT concept_id, data_partner_id, person_id, death_date, year_of_death, year_of_birth,
        CASE 
            WHEN year_of_birth IS NULL THEN 'birth error'
            WHEN year_of_death IS NULL THEN 'death error' 
            WHEN (year_of_death < year_of_birth OR year_of_death - year_of_birth > 130) THEN 'implausible death' 
        END AS error --error with death_date
        FROM (SELECT * , ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY person_id, data_partner_id) AS RN
        FROM death_inpatient)
        WHERE RN=1
        GROUP BY concept_id, data_partner_id, person_id, death_date, year_of_death, year_of_birth, error
        ORDER BY data_partner_id, concept_id, error --break down by data partner, concept, error