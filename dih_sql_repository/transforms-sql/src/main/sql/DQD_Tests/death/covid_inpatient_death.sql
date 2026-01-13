CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/death/covid_inpatient_death_1` AS
--count number of covid inpatient deaths per site
    SELECT data_partner_id, COUNT(*) AS num_covid_inpatient_death_by_site
    FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/death/no_dup`
    GROUP BY data_partner_id