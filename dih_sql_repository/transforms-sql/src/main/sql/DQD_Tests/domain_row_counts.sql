CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/domain_row_counts` AS

    SELECT 
          data_partner_id
        , count(*) as tot_num_records
        , 'measurement' as domain
    FROM `/UNITE/LDS/clean/measurement`
    GROUP BY data_partner_id

    UNION ALL

    SELECT 
          data_partner_id
        , count(*) as tot_num_records
        , 'condition_occurrence' as domain
    FROM `/UNITE/LDS/clean/condition_occurrence`
    GROUP BY data_partner_id

    UNION ALL

    SELECT 
          data_partner_id
        , count(*) as tot_num_records
        , 'drug_exposure' as domain
    FROM `/UNITE/LDS/clean/drug_exposure`
    GROUP BY data_partner_id

    UNION ALL

    SELECT 
          data_partner_id
        , count(*) as tot_num_records
        , 'observation' as domain
    FROM `/UNITE/LDS/clean/observation`
    GROUP BY data_partner_id

    UNION ALL

    SELECT 
          data_partner_id
        , count(*) as tot_num_records
        , 'procedure_occurrence' as domain
    FROM `/UNITE/LDS/clean/procedure_occurrence`
    GROUP BY data_partner_id

    UNION ALL

    SELECT
          data_partner_id
        , count(*) as tot_num_records
        , 'visit_occurrence' as domain
    FROM `/UNITE/LDS/clean/visit_occurrence`
    GROUP BY data_partner_id
