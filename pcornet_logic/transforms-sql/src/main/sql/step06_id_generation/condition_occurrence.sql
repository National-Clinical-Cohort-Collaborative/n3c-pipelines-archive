CREATE TABLE `ri.foundry.main.dataset.5089ce8b-2590-4350-b32b-b4a65270f5bd` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.7535361b-408a-40c4-8b34-3e782fc6c8c3` m
    LEFT JOIN `ri.foundry.main.dataset.23c7b053-1a7e-4aef-bcb8-ee2d73528f69` lookup
    ON m.condition_occurrence_id_51_bit = lookup.condition_occurrence_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as condition_occurrence_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(condition_occurrence_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
-- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
    , v.visit_occurrence_id
FROM global_id
INNER JOIN `ri.foundry.main.dataset.9051650f-db46-498d-b272-b654acea0e3f` p
    ON global_id.site_patid = p.site_patid
LEFT JOIN `ri.foundry.main.dataset.f1cd12a9-7491-4378-83de-97c582c67c7f` v
    ON global_id.site_encounterid = v.site_encounterid
    and v.domain_source != 'PROCEDURES'