CREATE TABLE `ri.foundry.main.dataset.46d64423-9961-4e85-bc80-39ea30a4dd85` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.7fb74da4-8574-4942-8592-34614b3d7735` m
    LEFT JOIN `ri.foundry.main.dataset.d9751581-d900-4365-bcae-e2aed069f09a` lookup
    ON m.location_id_51_bit = lookup.location_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as location_id 
    FROM (
        SELECT
            *
            -- Take collision index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(location_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT * FROM global_id
