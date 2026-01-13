CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/race_ethnicity_tests` AS


WITH totals_by_race_ethnicity AS
(  
    SELECT  data_partner_id,
            ethnicity_concept_name,
            race_concept_name,
            COUNT(*) AS count_people
    FROM `/UNITE/LDS/clean/person`
    GROUP BY data_partner_id,
            ethnicity_concept_name,
            race_concept_name
)


SELECT  r.*,
        t.number_of_people,
        ROUND((r.count_people * 100 / t.number_of_people), 2) AS percent_people
FROM totals_by_race_ethnicity r
LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site` t
ON r.data_partner_id = t.data_partner_id
ORDER BY data_partner_id, race_concept_name, ethnicity_concept_name