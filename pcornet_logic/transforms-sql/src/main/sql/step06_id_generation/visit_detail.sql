CREATE TABLE `ri.foundry.main.dataset.c3907188-89d4-4148-a8d1-3007c9f1d44d` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.248c6a6c-ed96-4960-8dc2-8ac16a830b64` m
    LEFT JOIN `ri.foundry.main.dataset.d45117b1-eb7f-48b8-be61-38d70bacb24d` lookup
    ON m.visit_detail_id_51_bit = lookup.visit_detail_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),
global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as visit_detail_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(visit_detail_id_51_bit, 2) + collision_index as local_id
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
INNER JOIN `ri.foundry.main.dataset.f1cd12a9-7491-4378-83de-97c582c67c7f` v
    ON global_id.site_encounter_id = v.site_encounterid 
    and v.domain_source != 'PROCEDURES'

    --care_site, caresite is not filled in for visit_detail
    --provider_id is not filled in for visit_detail
    --preceding_visit_detail_id from obs_gen is null for visit_detail
