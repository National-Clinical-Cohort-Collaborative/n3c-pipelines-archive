CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/death/death_date_error_1` AS
--count number of errors by type of errors per site
    SELECT data_partner_id, error, COUNT(error) AS num_error_by_type
    FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/death/no_dup`
    GROUP BY data_partner_id, error
    ORDER BY data_partner_id, error