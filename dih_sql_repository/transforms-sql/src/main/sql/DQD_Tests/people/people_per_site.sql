CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site` AS
    
    
        SELECT  data_partner_id,
            COUNT(*) AS number_of_people
        FROM `/UNITE/LDS/clean/person`
        GROUP BY data_partner_id

    
