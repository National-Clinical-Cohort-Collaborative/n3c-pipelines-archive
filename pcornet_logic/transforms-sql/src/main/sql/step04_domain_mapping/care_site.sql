CREATE TABLE `ri.foundry.main.dataset.3e850057-9683-46a7-8ba7-e73b301c918d` AS

with enc_map as (
  SELECT  
    cast(NULL as string) AS care_site_name,
    cast(fx.TARGET_CONCEPT_ID as int) AS place_of_service_concept_id,
    SUBSTRING(enc.facility_type, 1, 50) AS care_site_source_value,   
    SUBSTRING(enc.facility_type, 1, 50) AS place_of_service_source_value,  -- ehr/encounter
    'ENCOUNTER' AS domain_source, 
    data_partner_id,
    payload
    FROM (
      SELECT DISTINCT facility_type, data_partner_id, payload FROM `ri.foundry.main.dataset.710e7df0-575a-4b76-b799-bc0488f10e68` encounter 
        WHERE encounter.facility_type is not null 
    ) enc
    INNER JOIN `ri.foundry.main.dataset.9cd6e457-ddf3-416a-9bad-ffc36b30fcf3` fx 
      ON fx.CDM_TBL = 'ENCOUNTER' 
      AND fx.CDM_SOURCE = 'PCORnet' 
      AND fx.CDM_TBL_COLUMN_NAME = 'FACILITY_TYPE' 
      AND enc.facility_type = fx.SRC_CODE
)

SELECT 
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as care_site_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , care_site_name
    , place_of_service_concept_id
    , care_site_source_value
    , place_of_service_source_value
    , domain_source
    , data_partner_id
    , payload
    FROM (
        SELECT
          *
        , md5(concat_ws(
              ';'
            , COALESCE(care_site_name, '')
            , COALESCE(place_of_service_concept_id, '')
            , COALESCE(care_site_source_value, '')
            , COALESCE(place_of_service_source_value, '')
        )) as hashed_id
        FROM enc_map
    )
