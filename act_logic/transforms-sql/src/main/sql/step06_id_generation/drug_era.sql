CREATE TABLE `ri.foundry.main.dataset.f94f1e3a-1879-4690-8e04-e4738dfa126f` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.c8b7634a-883a-4c7e-9b95-77a5135ffc75` m
    LEFT JOIN `ri.foundry.main.dataset.422d849e-825d-4b37-82d4-b59fdfe045d7` lookup
    ON m.drug_era_id_51_bit = lookup.drug_era_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as drug_era_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(drug_era_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
-- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
FROM global_id
-- Inner join to remove patients who've been dropped in step04 due to not having an encounter
INNER JOIN `ri.foundry.main.dataset.ce0290bc-02a6-4fa7-883b-66c744f5a593` p
    ON global_id.site_patient_num = p.site_patient_num
