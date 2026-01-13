CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/covid_test_presence_and_value_check` AS


/* 

Use the codeset 724043329 (CD2H Labs, COVID-19 tests) to select all the COVID-19 test codes from the measurements dataset
Determine the total usage of each test code at each site, and the totals for each test result value

*/

SELECT      data_partner_id, 
            measurement_concept_id, 
            measurement_concept_name,
            value_as_concept_name,
            value_as_concept_id,
            COUNT(*) AS num_rows
     
          
FROM `/UNITE/LDS/clean/measurement` A

INNER JOIN 

(
    -- Get all the COVID tests from the most recent codesets for PCR and Ab tests
    SELECT concept_id AS covid_test_concept_id
    --FROM `/UNITE/N3C/Concept Set Ontology/hubble_base/concept_set_members`
    FROM `ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6`
    -- ATLAS SARS-CoV-2 rt-PCR and AG (Confirmed)
    -- Atlas #818 [N3C] CovidAntibody retry (Possible)
    --WHERE concept_set_name in ('ATLAS SARS-CoV-2 rt-PCR and AG', 'Atlas #818 [N3C] CovidAntibody retry', 'CovidAmbiguous')
    -- dropping the ambiguous as the home page is dropping this concept set
    WHERE concept_set_name in ('ATLAS SARS-CoV-2 rt-PCR and AG', 'Atlas #818 [N3C] CovidAntibody retry')
    AND is_most_recent_version = True
) B

ON A.measurement_concept_id = B.covid_test_concept_id

GROUP BY    data_partner_id, 
            measurement_concept_id, 
            measurement_concept_name,
            value_as_concept_name,
            value_as_concept_id

    