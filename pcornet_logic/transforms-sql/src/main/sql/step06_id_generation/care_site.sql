CREATE TABLE `ri.foundry.main.dataset.77dfa3b9-a0dd-4e68-ba60-19eed9e6e093` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.3e850057-9683-46a7-8ba7-e73b301c918d` m
    LEFT JOIN `ri.foundry.main.dataset.72c1fe18-352a-417b-b742-771314abbcc1` lookup
    ON m.care_site_id_51_bit = lookup.care_site_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as care_site_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(care_site_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    , cast(null as long) as location_id
FROM global_id
