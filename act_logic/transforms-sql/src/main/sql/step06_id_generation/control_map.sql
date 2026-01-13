CREATE TABLE `ri.foundry.main.dataset.00ed5001-cb0b-4196-8e5b-a2a2bce263bb` AS
      
WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.af0fa4e7-0756-46e5-b2ff-00337361efcd` m
    LEFT JOIN `ri.foundry.main.dataset.c0dbfba8-ca5c-44d9-9fde-0af65ae0973e` lookup
    ON m.control_map_id_51_bit = lookup.control_map_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as control_map_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(control_map_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    , p.person_id as case_person_id
    , pp.person_id as control_person_id
FROM global_id
INNER JOIN `ri.foundry.main.dataset.ce0290bc-02a6-4fa7-883b-66c744f5a593` p
    ON global_id.site_case_person_id = p.site_patient_num
LEFT JOIN `ri.foundry.main.dataset.ce0290bc-02a6-4fa7-883b-66c744f5a593` pp
    ON global_id.site_control_person_id = pp.site_patient_num
