CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/missingness/birthyears` AS

        WITH dob_q AS
        (
        SELECT data_partner_id,
                CASE
                        WHEN year_of_birth BETWEEN 1901 AND    2023 THEN 'Valid DOB'
                        ELSE 'Missing/Implausible DOB'
                END AS birthyear_quality
        FROM   `/UNITE/LDS/clean/person`
        )




        SELECT  d.*,
                count(*) AS count_birthyear,
                t.number_of_people,
                ROUND((count(*) * 100 / t.number_of_people), 2) AS percent_people
        FROM     dob_q as d
        LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site` t
                ON d.data_partner_id = t.data_partner_id
        GROUP BY d.data_partner_id,
                d.birthyear_quality,
                t.number_of_people
        ORDER BY d.data_partner_id,
                d.birthyear_quality 
        

