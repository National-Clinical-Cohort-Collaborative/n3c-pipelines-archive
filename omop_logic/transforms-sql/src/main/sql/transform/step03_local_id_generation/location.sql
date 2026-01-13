CREATE TABLE `ri.foundry.main.dataset.b1a4bb63-8f02-4ccb-9386-68d46eff7db8` AS

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as location_id_51_bit
    FROM (
        SELECT
              location_id as site_location_id
            , md5(CAST(location_id as string)) as hashed_id
            , address_1	
            , address_2	
            , city	
            , state	
            , zip	
            , county	
            , location_source_value	
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.13602758-7ebc-45d8-87d0-8a23c3a92258`
        WHERE location_id IS NOT NULL
    )   
