CREATE TABLE `ri.foundry.main.dataset.299f17c7-4dfa-4942-82ee-66021763120b` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.023f48b1-f14a-409e-a948-abe1e9b8c758` m
    LEFT JOIN `ri.foundry.main.dataset.af8ca11a-7564-47b1-b5c2-bef8fcfb1456` lookup
    ON m.provider_id_51_bit = lookup.provider_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as provider_id 
    FROM (
        SELECT
            *
            -- Take collision index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(provider_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT * FROM global_id