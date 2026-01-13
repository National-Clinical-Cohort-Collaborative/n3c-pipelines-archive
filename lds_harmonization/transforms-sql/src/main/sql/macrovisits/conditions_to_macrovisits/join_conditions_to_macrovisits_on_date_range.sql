CREATE TABLE `ri.foundry.main.dataset.a6bcb73f-ee56-436d-95dd-8b2221e491c9` AS
    
    -- starting with a map of microvisit to macrovisit, generate a table of
    -- distinct macrovisits 
    WITH macrovisits_only AS
    (
        SELECT DISTINCT person_id, macrovisit_id, macrovisit_start_date, macrovisit_end_date
        FROM `ri.foundry.main.dataset.de27cd23-ea8f-4d9f-825e-40b61cd2c1e2`
        WHERE macrovisit_id IS NOT NULL

    ),

    -- find conditions that lack a visit_occurrence_id but DO have a date
    c_with_date_but_not_visit_occurrence_id AS 
    (
        SELECT *
        FROM `ri.foundry.main.dataset.17e798ef-2459-4d3a-98ef-ac515b83871a`  
        WHERE visit_occurrence_id IS NULL
        AND condition_start_date IS NOT NULL
    )


    -- Join on date range when we can't join on microvisit_id

    SELECT  c.condition_occurrence_id,
            c.person_id,
            c.data_partner_id,
            c.condition_start_date,
            c.visit_occurrence_id,  -- these don't have a visit_occurrence_id
            v.macrovisit_id,
            v.macrovisit_start_date,
            v.macrovisit_end_date

    FROM c_with_date_but_not_visit_occurrence_id c
    INNER JOIN
    macrovisits_only v
    ON c.person_id = v.person_id
    AND c.condition_start_date BETWEEN v.macrovisit_start_date AND v.macrovisit_end_date

