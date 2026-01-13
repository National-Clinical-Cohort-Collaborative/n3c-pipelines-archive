CREATE TABLE `ri.foundry.main.dataset.ea98a439-a108-4927-a937-0be222cd0ada` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_MEDIUM, NUM_EXECUTORS_8') AS
    
  
    -- join all conditions that directly match a microvisit that is part of a macrovisit
    SELECT      c.condition_occurrence_id,
                c.person_id,
                c.data_partner_id,
                c.condition_start_date,
                c.visit_occurrence_id,
                v.macrovisit_id,
                v.macrovisit_start_date,
                v.macrovisit_end_date

    FROM  `ri.foundry.main.dataset.17e798ef-2459-4d3a-98ef-ac515b83871a` c
    INNER JOIN `ri.foundry.main.dataset.de27cd23-ea8f-4d9f-825e-40b61cd2c1e2` v
    ON c.visit_occurrence_id = v.visit_occurrence_id
