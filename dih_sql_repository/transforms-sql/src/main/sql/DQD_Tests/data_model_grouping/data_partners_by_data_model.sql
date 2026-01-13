CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/data_model_grouping/data_partners_by_data_model` AS
    
    SELECT  cdm_name,
            released,
            COUNT(*) as num_sites
    FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/manifest ontology/manifest`
    GROUP BY cdm_name, released

    