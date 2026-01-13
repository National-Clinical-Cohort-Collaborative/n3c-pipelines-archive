CREATE TABLE `ri.foundry.main.dataset.8527b64b-1934-4d60-a6a4-8b3c9c863987` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.db7a68ca-7ba8-4bfb-ae11-49731d5ffd05` d
    LEFT JOIN `ri.foundry.main.dataset.5c84bb49-1f94-4ea9-9c88-4601f7cfe993` lookup
    ON d.observation_period_id_51_bit = lookup.observation_period_id_51_bit
    AND d.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as observation_period_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(observation_period_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final person ids from the final OMOP domains after collision resolutions
    , p.person_id
FROM global_id
LEFT JOIN `ri.foundry.main.dataset.3b888e3f-a7f7-4251-a585-b0d0d987e29d` p
ON global_id.site_person_id = p.site_person_id
