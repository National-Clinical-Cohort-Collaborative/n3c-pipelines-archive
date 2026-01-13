CREATE TABLE `ri.foundry.main.dataset.c5d902c7-365f-48f1-a021-11be8d519738` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.0164a55d-e593-4508-b2b5-7cd9bb2f2bbe` m
    LEFT JOIN `ri.foundry.main.dataset.37132de2-d82c-43b1-bfda-c282e5861047` lookup
    ON m.drug_exposure_id_51_bit = lookup.drug_exposure_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as drug_exposure_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(drug_exposure_id_51_bit, 2) + collision_index as local_id
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