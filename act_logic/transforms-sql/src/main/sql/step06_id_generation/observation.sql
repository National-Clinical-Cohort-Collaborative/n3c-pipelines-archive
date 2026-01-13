CREATE TABLE `ri.foundry.main.dataset.be28e804-88a2-44ea-9c77-07c66123cfc7` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.39bcd55d-24d4-49c0-bc32-2d02c8f7a4bb` m
    LEFT JOIN `ri.foundry.main.dataset.f180bfbb-ba1c-4466-ab66-a4ac8259ac1e` lookup
    ON m.observation_id_51_bit = lookup.observation_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as observation_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(observation_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
    , v.visit_occurrence_id
FROM global_id
-- Inner join to remove patients who've been dropped in step04 due to not having an encounter
INNER JOIN `ri.foundry.main.dataset.ce0290bc-02a6-4fa7-883b-66c744f5a593` p
    ON global_id.site_patient_num = p.site_patient_num
LEFT JOIN `ri.foundry.main.dataset.f7fc5c89-ff9c-4399-88f7-1f806a9bc01b` v
    ON v.site_encounter_num = global_id.site_encounter_num  and v.site_patient_num = global_id.site_patient_num
    and v.domain_source != 'OBSERVATION_FACT'