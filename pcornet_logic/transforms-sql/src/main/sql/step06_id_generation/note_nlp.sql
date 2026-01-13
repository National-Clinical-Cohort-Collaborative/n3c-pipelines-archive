CREATE TABLE `ri.foundry.main.dataset.49379424-580f-4547-8174-c59cb7b55c6b`
TBLPROPERTIES (foundry_transform_profile = 'EXECUTOR_MEMORY_LARGE') AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.830300aa-2728-438d-904a-4a228d3e569f` d
    LEFT JOIN `ri.foundry.main.dataset.45ed6bc4-03ca-4824-b48a-7111e283ae10` lookup
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
LEFT JOIN `ri.foundry.main.dataset.7d414175-553f-4dd5-a86f-b9e93e13df05` note
ON global_id.site_note_id = note.site_note_id


