CREATE TABLE `ri.foundry.main.dataset.571f441c-deab-49ef-ba20-6319af47b3bb` AS


        WITH totals_by_ethnicity AS
        (  
        SELECT  data_partner_id,
                CASE 
                        WHEN ethnicity_concept_name = 'No information' THEN 'Missing/Unknown'
                        WHEN ethnicity_concept_name = 'No matching concept' THEN 'Missing/Unknown'
                        WHEN ethnicity_concept_name = 'Other' THEN 'Missing/Unknown'
                        WHEN ethnicity_concept_name = 'Other/Unknown' THEN 'Missing/Unknown'
                        WHEN ethnicity_concept_name = 'Patient ethnicity unknown' THEN 'Missing/Unknown'
                        WHEN ethnicity_concept_name = 'Refuse to answer' THEN 'Missing/Unknown'
                        WHEN ethnicity_concept_name = 'Unknown' THEN 'Missing/Unknown'
                        WHEN ethnicity_concept_name IS NULL THEN 'Missing/Unknown'
                        ELSE ethnicity_concept_name
                END AS ethnicity_concept_name,
                COUNT(*) AS count_people
        FROM `ri.foundry.main.dataset.456df227-b15d-4356-9df6-b113e90eb239`
        GROUP BY data_partner_id,
                CASE 
                        WHEN ethnicity_concept_name = 'No information' THEN 'Missing/Unknown'
                        WHEN ethnicity_concept_name = 'No matching concept' THEN 'Missing/Unknown'
                        WHEN ethnicity_concept_name = 'Other' THEN 'Missing/Unknown'
                        WHEN ethnicity_concept_name = 'Other/Unknown' THEN 'Missing/Unknown'
                        WHEN ethnicity_concept_name = 'Patient ethnicity unknown' THEN 'Missing/Unknown'
                        WHEN ethnicity_concept_name = 'Refuse to answer' THEN 'Missing/Unknown'
                        WHEN ethnicity_concept_name = 'Unknown' THEN 'Missing/Unknown'
                        WHEN ethnicity_concept_name IS NULL THEN 'Missing/Unknown'
                        ELSE ethnicity_concept_name
                END
        )
        
        SELECT  r.*,
                t.number_of_people,
                ROUND((r.count_people* 100 / t.number_of_people), 2)  AS percent_people
        FROM totals_by_ethnicity r
        LEFT JOIN `ri.foundry.main.dataset.a8155787-cace-4c2b-8210-71e8356ab9b9` t
        ON r.data_partner_id = t.data_partner_id
        ORDER BY data_partner_id, ethnicity_concept_name

