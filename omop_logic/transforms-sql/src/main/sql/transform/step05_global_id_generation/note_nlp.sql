CREATE TABLE `ri.foundry.main.dataset.512633c0-74d3-4378-8197-af9142537523` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.ed167d32-dd98-4eb9-b2aa-1c4fba7a822d` d
    LEFT JOIN `ri.foundry.main.dataset.ecc602fd-3632-49bb-9198-329e5a199cfe` lookup
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
LEFT JOIN `ri.foundry.main.dataset.19e13d70-3b53-4995-955d-a75d65dfbebe` note
ON global_id.site_note_id = note.site_note_id


