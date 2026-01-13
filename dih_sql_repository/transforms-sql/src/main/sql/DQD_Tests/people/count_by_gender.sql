CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/count_by_gender` AS
    
    
    WITH people_per_gender AS
    (
            SELECT   data_partner_id,
                    CASE
                            WHEN gender_concept_name = 'FEMALE' THEN 'Female'
                            WHEN gender_concept_name = 'MALE' THEN 'Male'
                            ELSE 'Other/Missing/Unknown'
                    END      AS gender_concept_name,
                    Count(*) AS num_people
            FROM     `/UNITE/LDS/clean/person`
            GROUP BY data_partner_id,
                    gender_concept_name )
                    
    SELECT    pg.data_partner_id,
            gender_concept_name,
            SUM(pg.num_people) AS num_people,
            ps.number_of_people                                   AS total_people,
            round(SUM((pg.num_people* 100 / ps.number_of_people)), 2) AS percent_people
    FROM      people_per_gender pg
    LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site` ps
    ON pg.data_partner_id = ps.data_partner_id
    GROUP BY pg.data_partner_id,
            gender_concept_name,
            ps.number_of_people
