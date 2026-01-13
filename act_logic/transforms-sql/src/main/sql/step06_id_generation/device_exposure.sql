CREATE TABLE `ri.foundry.main.dataset.3d8ce476-6f4b-47fa-b84b-bbd6867dcd79` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.c7178829-a2f6-4af4-a236-642273ec26de` m
    LEFT JOIN `ri.foundry.main.dataset.0162aa8b-f2a9-4eba-adeb-7bf8f141e41b` lookup
    ON m.device_exposure_id_51_bit = lookup.device_exposure_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as device_exposure_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(device_exposure_id_51_bit, 2) + collision_index as local_id
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