CREATE TABLE `ri.foundry.main.dataset.50ebb3c2-f48b-43e6-bfbd-6f47e3a33f5f` AS

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as care_site_id_51_bit
    FROM (
        SELECT
              care_site_id as site_care_site_id
            , md5(CAST(care_site_id as string)) as hashed_id
            , care_site_name
            , place_of_service_concept_id
            , location_id as site_location_id
            , care_site_source_value
            , place_of_service_source_value
            , data_partner_id
            , payload
            --from ri.foundry.main.dataset.962c7a8f-368a-446e-abb5-9efd5df78457
        FROM `ri.foundry.main.dataset.962c7a8f-368a-446e-abb5-9efd5df78457`
        WHERE care_site_id IS NOT NULL
    )   