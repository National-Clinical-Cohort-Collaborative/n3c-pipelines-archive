CREATE TABLE `ri.foundry.main.dataset.53d0cdaa-c42f-4017-b1e9-4c74f4b6d6a1` TBLPROPERTIES (foundry_transform_profile = 'NUM_EXECUTORS_4') AS

    
    -- starting with a map of microvisit to macrovisit, generate a table of
    -- distinct macrovisits 
    WITH macrovisits_only AS
    (
        SELECT DISTINCT person_id, macrovisit_id, macrovisit_start_date, macrovisit_end_date
        FROM `ri.foundry.main.dataset.de27cd23-ea8f-4d9f-825e-40b61cd2c1e2`
        WHERE macrovisit_id IS NOT NULL

    ),

    -- find measurements that lack a visit_occurrence_id but DO have a date
    me_with_date_but_not_visit_occurrence_id AS 
    (
        SELECT *
        FROM `ri.foundry.main.dataset.0f30f7f8-dc17-4f19-b9a7-1297708050ff`  
        WHERE visit_occurrence_id IS NULL
        AND measurement_date IS NOT NULL
    )


    -- Join on date range when we can't join on microvisit_id

    SELECT      me.measurement_id,
                me.person_id,
                me.data_partner_id,
                me.measurement_date,
                me.visit_occurrence_id,
                v.macrovisit_id,
                v.macrovisit_start_date,
                v.macrovisit_end_date

    FROM me_with_date_but_not_visit_occurrence_id me
    INNER JOIN
    macrovisits_only v
    ON me.person_id = v.person_id
    AND me.measurement_date BETWEEN v.macrovisit_start_date AND v.macrovisit_end_date

