CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/covid_test_presence_and_value_check_with_LOINC_percent` AS

/* use the following dataset for covid tests
`/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/covid_test_presence_and_value_check` 
*/
with total_covid_test_num_by_site as (
SELECT data_partner_id,
sum(num_rows) as totalcovidtestnum
FROM `ri.foundry.main.dataset.e3130ed4-c1a4-4178-bd98-cbca4d53435a`
group by data_partner_id
),
-- covid test result counts
total_covid_test_result_num  as 
(
     SELECT DISTINCT  data_partner_id, 
            value_as_concept_name,
            value_as_concept_id,
            COUNT(*) AS covid_test_result_num_rows
      FROM `/UNITE/LDS/clean/measurement` m 
      where m.measurement_concept_id in ( 
         SELECT concept_id
         --FROM `/UNITE/N3C/Concept Set Ontology/hubble_base/concept_set_members`
         FROM `ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6`
         -- ATLAS SARS-CoV-2 rt-PCR and AG (Confirmed)
         -- Atlas #818 [N3C] CovidAntibody retry (Possible)
         WHERE concept_set_name in ('ATLAS SARS-CoV-2 rt-PCR and AG', 'Atlas #818 [N3C] CovidAntibody retry')
         AND is_most_recent_version = True )
      group by data_partner_id, 
            value_as_concept_name,
            value_as_concept_id
),

total_covid_test_result_num_by_site as (
SELECT data_partner_id,
            value_as_concept_name,
            value_as_concept_id,
sum(covid_test_result_num_rows) AS totalcovidtestresultnum
FROM total_covid_test_result_num
group by data_partner_id,
    value_as_concept_name,
    value_as_concept_id
)

-- percent for each test first by data_partner_id
select  c.data_partner_id, 
            --measurement_concept_id, 
            --measurement_concept_name,
            value_as_concept_name,
            value_as_concept_id,
            totalcovidtestresultnum, -- covid test result
            totalcovidtestnum, -- covid test
            ROUND((r.totalcovidtestresultnum / c.totalcovidtestnum ) * 100, 2) as percent_covid_test_results    
from total_covid_test_num_by_site c
join total_covid_test_result_num_by_site r on c.data_partner_id = r.data_partner_id


    