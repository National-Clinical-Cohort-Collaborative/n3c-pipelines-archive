CREATE TABLE `ri.foundry.main.dataset.8646a54c-e315-4fdc-92c0-aef00f817c23` AS
    SELECT 
        * 
        , CAST(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as control_map_id_51_bit
    FROM (
        SELECT
            control_map_id as site_control_map_id
            , md5(CAST(control_map_id AS STRING)) as hashed_id
            , case_person_id as site_case_person_id
            , buddy_num
            , control_person_id as site_control_person_id   
            , CAST(NULL as INTEGER) as case_age	
            , CAST(NULL as STRING) as case_sex	
            , CAST(NULL as STRING) as case_race	
            , CAST(NULL as STRING) as case_ethn	
            , CAST(NULL as INTEGER) as control_age	
            , CAST(NULL as STRING) as control_sex	
            , CAST(NULL as STRING) as control_race	
            , CAST(NULL as STRING) as control_ethn
            , data_partner_id
            , payload
        FROM ( 
            SELECT
            CAST( (case_person_id || buddy_num || control_person_id) AS STRING) as control_map_id ,
            case_person_id,
            buddy_num, 
            control_person_id, 
            data_partner_id,
            payload
            FROM (
                select distinct case_person_id, buddy_num, control_person_id, data_partner_id, payload
                from 
                `ri.foundry.main.dataset.2d44bd1b-5fe5-402b-8e13-1136787c27a6` cm
                WHERE case_person_id IS NOT NULL and buddy_num is not null and control_person_id is not null 
            ) 
        )
    )