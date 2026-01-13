CREATE TABLE `ri.foundry.main.dataset.9d927f90-4b58-4802-96d4-d840c7814a9b` TBLPROPERTIES (foundry_transform_profile = 'NUM_EXECUTORS_4') AS
    
    
    -- starting with a map of microvisit to macrovisit, generate a table of
    -- distinct macrovisits 
    WITH macrovisits_only AS
    (
        SELECT DISTINCT person_id, macrovisit_id, macrovisit_start_date, macrovisit_end_date
        FROM `ri.foundry.main.dataset.de27cd23-ea8f-4d9f-825e-40b61cd2c1e2`
        WHERE macrovisit_id IS NOT NULL

    ),

    -- find procedures that lack a visit_occurrence_id but DO have a date
    p_with_date_but_not_visit_occurrence_id AS 
    (
        SELECT *
        FROM `ri.foundry.main.dataset.7394de3a-f3fe-4b73-9e61-12858dcd9868`  
        WHERE visit_occurrence_id IS NULL
        AND procedure_date IS NOT NULL
    )


    -- Join on date range when we can't join on microvisit_id

    SELECT      p.procedure_occurrence_id,
                p.person_id,
                p.data_partner_id,
                p.procedure_date,
                p.visit_occurrence_id,
                v.macrovisit_id,
                v.macrovisit_start_date,
                v.macrovisit_end_date

    FROM p_with_date_but_not_visit_occurrence_id p
    INNER JOIN
    macrovisits_only v
    ON p.person_id = v.person_id
    AND p.procedure_date BETWEEN v.macrovisit_start_date AND v.macrovisit_end_date

