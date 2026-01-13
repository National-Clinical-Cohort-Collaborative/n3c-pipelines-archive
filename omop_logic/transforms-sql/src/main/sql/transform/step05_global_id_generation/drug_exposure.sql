CREATE TABLE `ri.foundry.main.dataset.6d188b44-0fb8-4281-ac85-4b006f1902bc`
TBLPROPERTIES (foundry_transform_profile = 'EXECUTOR_MEMORY_MEDIUM') AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.0da5f597-e7d7-471b-8613-883c3601e1ec` d
    LEFT JOIN `ri.foundry.main.dataset.5f6e2fa9-8bbb-442d-ada5-3fc804c5fed5` lookup
    ON d.drug_exposure_id_51_bit = lookup.drug_exposure_id_51_bit
    AND d.hashed_id = lookup.hashed_id
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
    , prov.provider_id
FROM global_id
LEFT JOIN `ri.foundry.main.dataset.3b888e3f-a7f7-4251-a585-b0d0d987e29d` p
ON global_id.site_person_id = p.site_person_id
LEFT JOIN `ri.foundry.main.dataset.e5f71652-b192-48f9-aff4-b68c8ebd576a` v
ON global_id.site_visit_occurrence_id = v.site_visit_occurrence_id
LEFT JOIN `ri.foundry.main.dataset.841cfefa-0485-48b7-a360-f359ab610eac` prov
ON global_id.site_provider_id = prov.site_provider_id