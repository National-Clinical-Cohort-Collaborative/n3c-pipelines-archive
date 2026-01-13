CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/death/covid_positive` AS
--identify covid positive patients
WITH A AS (
    SELECT DISTINCT c.condition_concept_id as concept_id, c.data_partner_id, c.person_id, c.visit_occurrence_id
    FROM `/UNITE/LDS/clean/condition_occurrence` c
    WHERE c.condition_concept_id IN (4126681, 9191, 36032716, 45884084, 36715206, 45877985, 45878745, 45881802, 37311061) --concept_id of COVID positive in condition domain
    AND c.visit_occurrence_id IS NOT NULL
),

B AS (
    SELECT DISTINCT m.measurement_concept_id as concept_id, m.data_partner_id, m.person_id, m.visit_occurrence_id
    FROM `/UNITE/LDS/clean/measurement` m
    WHERE m.measurement_concept_id IN (SELECT DISTINCT concept_id 
        FROM `/N3C Export Area/Concept Set Ontology/Concept Set Ontology/hubble_base/concept_set_members` 
        WHERE codeset_id IN (386776576, 263281373) AND is_most_recent_version = true) 
        AND m.visit_occurrence_id IS NOT NULL
) --measurement concepts from concept sets ATLAS SARS-CoV-2 rt-PCR and AG, Atlas #818 [N3C] CovidAntibody retry

    SELECT A.*
    FROM A
    FULL OUTER JOIN B
    ON A.person_id = B.person_id AND A.data_partner_id = B.data_partner_id