CREATE TABLE `ri.foundry.main.dataset.c89e069f-b34d-4a2d-9f21-ed7f3946503a` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.61b4488b-3eb8-40c8-8fb4-028b2cf336d0` m
    LEFT JOIN `ri.foundry.main.dataset.ccbbb3d1-1dc0-42b5-ae1f-f698bac6f6fe` lookup
    ON m.measurement_id_51_bit = lookup.measurement_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as measurement_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(measurement_id_51_bit, 2) + collision_index as local_id
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