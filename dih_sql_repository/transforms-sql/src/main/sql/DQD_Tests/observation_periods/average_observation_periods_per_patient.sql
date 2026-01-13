CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/observation_periods/average_observation_periods_per_patient` TBLPROPERTIES (foundry_transform_profile = 'NUM_EXECUTORS_4') AS
    SELECT 
          DISTINCT p.data_partner_id
        , CASE 
            WHEN c.avg_periods_per_patient IS NULL THEN 0
            ELSE c.avg_periods_per_patient END
        AS avg_periods_per_patient
    FROM `/UNITE/LDS/clean/person` p
    LEFT JOIN (
        SELECT 
          data_partner_id, 
          avg(num_periods) as avg_periods_per_patient 
        FROM (
            SELECT person_id, data_partner_id, count(*) as num_periods FROM `/UNITE/LDS/clean/observation_period`
            GROUP BY person_id, data_partner_id
        )
        GROUP BY data_partner_id
    ) c
    ON p.data_partner_id = c.data_partner_id
