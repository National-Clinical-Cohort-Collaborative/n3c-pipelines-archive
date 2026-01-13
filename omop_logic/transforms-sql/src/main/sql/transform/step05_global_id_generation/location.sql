CREATE TABLE `ri.foundry.main.dataset.cf7fb682-fd88-4cc9-b6a4-d86a275692c4` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.b1a4bb63-8f02-4ccb-9386-68d46eff7db8` d
    LEFT JOIN `ri.foundry.main.dataset.57fe16e3-cccb-4849-b3e4-1c2ef6b4d3eb` lookup
    ON d.location_id_51_bit = lookup.location_id_51_bit
    AND d.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as location_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(location_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT * FROM global_id
