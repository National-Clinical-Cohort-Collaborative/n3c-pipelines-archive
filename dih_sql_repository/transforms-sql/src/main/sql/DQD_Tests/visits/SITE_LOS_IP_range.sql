CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/visits/SITE_LOS_IP_range.sql` AS

    SELECT      data_partner_id,
                ROUND(AVG(LOS),0) AS mean_LOS,
                MIN(LOS) AS min_LOS,
                ROUND(PERCENTILE(LOS, 0.5),0) AS median_LOS,
                MAX(LOS) AS max_LOS
    FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/visits/Site_LOS_IP`
    WHERE LOS >-1 AND LOS < 4000 -- THIS IS ARBITRARY
    GROUP BY data_partner_id
    ORDER BY data_partner_id
