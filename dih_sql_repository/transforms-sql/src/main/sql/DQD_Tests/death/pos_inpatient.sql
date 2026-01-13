CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/death/pos_inpatient` AS
--merge between positive patients and inpatient visit
    SELECT DISTINCT c.*, i.visit_concept_id
    FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/death/covid_positive` c
    INNER JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/death/inpatient_admitted` i
    ON c.person_id = i.person_id AND c.data_partner_id = i.data_partner_id
