CREATE TABLE `ri.foundry.main.dataset.fc27922f-3361-496e-8cce-6bd65e6a9e20` AS

SELECT 
    d.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
FROM `ri.foundry.main.dataset.cd7d81b2-7251-4f80-bc2b-ed78305fd394` d
LEFT JOIN `ri.foundry.main.dataset.3b888e3f-a7f7-4251-a585-b0d0d987e29d` p
ON d.site_person_id = p.site_person_id 
