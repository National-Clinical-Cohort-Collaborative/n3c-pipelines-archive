CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/birthdates` AS
    

    WITH people_per_month AS
    (
        SELECT  month_of_birth,
            data_partner_id,
            COUNT(*) AS num_people
    
        FROM `/UNITE/LDS/clean/person`
        GROUP BY month_of_birth, data_partner_id
    )

    SELECT  pm.data_partner_id,
            month_of_birth,
            pm.num_people,
            ps.number_of_people AS total_people,
            ROUND((pm.num_people / ps.number_of_people), 3) * 100 AS percent_people
    FROM people_per_month pm
    LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site` ps
    ON pm.data_partner_id = ps.data_partner_id