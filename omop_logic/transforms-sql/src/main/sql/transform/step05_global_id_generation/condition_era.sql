CREATE TABLE `ri.foundry.main.dataset.b1b6ce48-40be-4b25-9abc-f712e93a940f` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.e6ab2e8a-0d19-432c-9e33-989e42cf866e` d
    LEFT JOIN `ri.foundry.main.dataset.88acfe5a-f9b8-443c-8d45-068903f1b617` lookup
    ON d.condition_era_id_51_bit = lookup.condition_era_id_51_bit
    AND d.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as condition_era_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(condition_era_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
FROM global_id
LEFT JOIN `ri.foundry.main.dataset.3b888e3f-a7f7-4251-a585-b0d0d987e29d` p
ON global_id.site_person_id = p.site_person_id
