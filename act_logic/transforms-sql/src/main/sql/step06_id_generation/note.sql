CREATE TABLE `ri.foundry.main.dataset.a25241b6-f462-4e09-b3f9-b10886f64f31` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.77d18d50-9339-460d-85ee-b5d3d0effddc` d
    LEFT JOIN `ri.foundry.main.dataset.f84d43de-a89e-41a6-bbe5-f54b20d19ace` lookup
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
LEFT JOIN `ri.foundry.main.dataset.ce0290bc-02a6-4fa7-883b-66c744f5a593` p
ON global_id.site_person_id = p.site_patient_num
LEFT JOIN `ri.foundry.main.dataset.f7fc5c89-ff9c-4399-88f7-1f806a9bc01b` v
ON global_id.site_visit_occurrence_id = v.site_encounter_num
