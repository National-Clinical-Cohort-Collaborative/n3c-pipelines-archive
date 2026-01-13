CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/omop-non-standard-concepts/omop_vocabuary_version` AS
    SELECT distinct data_partner_id, cdm_name, vocabulary_version
    FROM `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683`
    where cdm_name = 'OMOP'