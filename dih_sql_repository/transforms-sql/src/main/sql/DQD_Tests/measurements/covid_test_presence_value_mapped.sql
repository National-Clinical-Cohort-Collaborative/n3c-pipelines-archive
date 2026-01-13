CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/covid_test_presence_value_mapped` AS
    SELECT
    c.measurement_concept_name,
    m.TestGroup,
    c.data_partner_id,
    c.measurement_concept_id,
    c.num_rows,
    c.value_as_concept_name
    FROM `ri.foundry.main.dataset.e3130ed4-c1a4-4178-bd98-cbca4d53435a`  c
    INNER JOIN `ri.foundry.main.dataset.c3aebef7-8250-47f0-be8b-d0f1fb46d17a`  m
    ON c.measurement_concept_name = m.LongCommonNames