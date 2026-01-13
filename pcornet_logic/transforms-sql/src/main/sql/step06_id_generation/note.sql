CREATE TABLE `ri.foundry.main.dataset.7d414175-553f-4dd5-a86f-b9e93e13df05` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.35175458-8ed6-485a-b923-007329888b9f` d
    LEFT JOIN `ri.foundry.main.dataset.8e0efa6f-cbc9-48e7-9a2c-9c1a09e64b11` lookup
    ON d.note_id_51_bit = lookup.note_id_51_bit
    AND d.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as note_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(note_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
    , v.visit_occurrence_id
FROM global_id
LEFT JOIN `ri.foundry.main.dataset.9051650f-db46-498d-b272-b654acea0e3f` p
ON global_id.site_person_id = p.site_patid
LEFT JOIN `ri.foundry.main.dataset.f1cd12a9-7491-4378-83de-97c582c67c7f` v
ON global_id.site_visit_occurrence_id = v.site_encounterid
