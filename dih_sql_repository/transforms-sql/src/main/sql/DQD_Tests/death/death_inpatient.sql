CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/death/death_inpatient` AS
--merge covid positive with inpatient visits table and death table

    SELECT DISTINCT p.concept_id, p.data_partner_id, p.person_id, p.visit_concept_id, d.death_date, d.year_of_death, d.year_of_birth
    FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/death/pos_inpatient` p
    INNER JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/death/death` d
    ON p.person_id = d.person_id AND p.data_partner_id = d.data_partner_id