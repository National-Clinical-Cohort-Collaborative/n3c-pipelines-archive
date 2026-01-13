CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/death/total_pos_inpat` AS
    SELECT COUNT(*) AS total, data_partner_id
    FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/death/pos_inpatient`
    GROUP BY data_partner_id