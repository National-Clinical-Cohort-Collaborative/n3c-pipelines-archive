CREATE TABLE `ri.foundry.main.dataset.8357948c-1e31-4aa0-b0a8-637206021265` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.4f6d6672-d20a-4cb4-9260-ed05a3ee8ac5` d
    LEFT JOIN `ri.foundry.main.marketplace-repository-integration.a5f1a1f6-3f02-4007-83ae-ba51663eb2d6_nlp` lookup
    ON d.note_nlp_id_51_bit = lookup.note_nlp_id_51_bit
    AND d.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as note_nlp_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(note_nlp_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final provider, and visit and visit detail ids from the final OMOP domains after collision resolutions
    , note.note_id
FROM global_id
LEFT JOIN `ri.foundry.main.dataset.a25241b6-f462-4e09-b3f9-b10886f64f31` note
ON global_id.site_note_id = note.site_note_id


