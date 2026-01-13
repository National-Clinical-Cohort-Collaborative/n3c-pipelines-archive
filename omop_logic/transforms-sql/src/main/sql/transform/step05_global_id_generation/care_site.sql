CREATE TABLE `ri.foundry.main.dataset.267c41ad-bc83-438b-8f99-ad573fc7a7f9` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.50ebb3c2-f48b-43e6-bfbd-6f47e3a33f5f` d
    LEFT JOIN `ri.foundry.main.dataset.dca7be93-48e6-4509-90c7-cf2ac2aa4e6e` lookup
    ON d.care_site_id_51_bit = lookup.care_site_id_51_bit
    AND d.hashed_id = lookup.hashed_id
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
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , loc.location_id
FROM global_id
LEFT JOIN `ri.foundry.main.dataset.cf7fb682-fd88-4cc9-b6a4-d86a275692c4` loc
ON global_id.site_location_id = loc.site_location_id