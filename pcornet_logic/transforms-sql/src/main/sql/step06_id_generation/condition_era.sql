CREATE TABLE `ri.foundry.main.dataset.e0831d56-f894-461d-8c20-fd534ccfe845` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.a1945c5e-2642-448d-963f-2a74fe805d5c` m
    LEFT JOIN `ri.foundry.main.dataset.7974e8fa-7f7b-46ca-ac0f-64e0e154d7a0` lookup
    ON m.condition_era_id_51_bit = lookup.condition_era_id_51_bit
    AND m.hashed_id = lookup.hashed_id
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
-- Inner join to remove patients who've been dropped in step04 due to not having an encounter
INNER JOIN `ri.foundry.main.dataset.9051650f-db46-498d-b272-b654acea0e3f` p
    ON global_id.site_patid = p.site_patid
