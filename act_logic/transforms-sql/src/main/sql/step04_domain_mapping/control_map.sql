CREATE TABLE `ri.foundry.main.dataset.af0fa4e7-0756-46e5-b2ff-00337361efcd` AS
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
                CAST( (case_patid || buddy_num || control_patid) AS STRING) as control_map_id ,
                case_patid as case_person_id,
                buddy_num, 
                control_patid as control_person_id, 
                data_partner_id,
                payload
                FROM (
                    select distinct case_patid, buddy_num, control_patid, data_partner_id, payload
                    from 
                    `ri.foundry.main.dataset.6a989015-2414-4122-82db-1ed1b08233f8` cm
                    WHERE case_patid IS NOT NULL and buddy_num is not null and control_patid is not null 
            ) 
        )
    )        