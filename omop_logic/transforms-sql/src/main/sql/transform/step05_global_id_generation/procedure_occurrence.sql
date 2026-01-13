CREATE TABLE `ri.foundry.main.dataset.853f4c7d-5820-4aff-9337-0447e01d6ae0` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.184e67c2-db8d-4476-addd-58eca4ab5d89` d
    LEFT JOIN `ri.foundry.main.dataset.40396ebe-45ee-4131-a76e-2cd9c4f1f819` lookup
    ON d.procedure_occurrence_id_51_bit = lookup.procedure_occurrence_id_51_bit
    AND d.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as procedure_occurrence_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(procedure_occurrence_id_51_bit, 2) + collision_index as local_id
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