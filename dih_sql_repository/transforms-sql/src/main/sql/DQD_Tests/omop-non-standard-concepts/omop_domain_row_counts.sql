CREATE TABLE `ri.foundry.main.dataset.b3558700-d4b5-43d1-9e0e-05a49feeb717` AS
   
    SELECT 
          data_partner_id
        , count(*) as tot_num_records
        , 'measurement' as domain
    FROM `ri.foundry.main.dataset.1937488e-71c5-4808-bee9-0f71fd1284ac`
    WHERE data_partner_id in ( 
      SELECT data_partner_id from `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683`
      WHERE cdm_name = 'OMOP')
    GROUP BY data_partner_id

    UNION ALL

    SELECT 
          data_partner_id
        , count(*) as tot_num_records
        , 'condition_occurrence' as domain
    FROM `ri.foundry.main.dataset.641c645d-eacf-4a5a-983d-3296e1641f0c`
    WHERE data_partner_id in ( 
      SELECT data_partner_id from `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683`
      WHERE cdm_name = 'OMOP')
    GROUP BY data_partner_id

    UNION ALL

    SELECT 
          data_partner_id
        , count(*) as tot_num_records
        , 'drug_exposure' as domain
    FROM `ri.foundry.main.dataset.3feda4dc-5389-4521-ab9e-0361ea5773fd`
    WHERE data_partner_id in ( 
      SELECT data_partner_id from `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683`
      WHERE cdm_name = 'OMOP')
    GROUP BY data_partner_id

    UNION ALL

    SELECT 
          data_partner_id
        , count(*) as tot_num_records
        , 'observation' as domain
    FROM `ri.foundry.main.dataset.f925d142-3b25-498e-8538-4b9685c5270f`
    WHERE data_partner_id in ( 
      SELECT data_partner_id from `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683`
      WHERE cdm_name = 'OMOP')
    GROUP BY data_partner_id

    UNION ALL

    SELECT 
          data_partner_id
        , count(*) as tot_num_records
        , 'procedure_occurrence' as domain
    FROM `ri.foundry.main.dataset.2a3c8355-7fbf-478f-bbbf-eabb6733b04d`
    WHERE data_partner_id in ( 
      SELECT data_partner_id from `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683`
      WHERE cdm_name = 'OMOP')
    GROUP BY data_partner_id

    UNION ALL

    SELECT
          data_partner_id
        , count(*) as tot_num_records
        , 'visit_occurrence' as domain
    FROM `ri.foundry.main.dataset.749e3f71-9b0e-4896-af8b-91aefe71b308`
    WHERE data_partner_id in ( 
      SELECT data_partner_id from `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683`
      WHERE cdm_name = 'OMOP')
    GROUP BY data_partner_id
