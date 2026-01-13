-- CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/standard_concept_checks/identify_nonstd_concepts` AS
CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/identify_nonstd_concepts` TBLPROPERTIES (foundry_transform_profiles = 'NUM_EXECUTORS_8, EXECUTOR_MEMORY_SMALL') AS
WITH
-- OK_LOCAL_CONCEPT_IDS are (2004208004, 2004208005, 2004207791 )
CO_COUNTS as (
        SELECT  co.data_partner_id,  count(*) as ct,  
                co.condition_concept_id as d_concept_id, co.condition_source_concept_id as d_source_concept_id,  
                co.condition_source_concept_name as d_source_concept_name,
                c.concept_name, c.vocabulary_id, c.concept_code,
        CASE -- each includes nc.condition_concept_id is not null b/c it's in the WHERE clause
             when c.concept_id = 0  then 'no matching concept'
             when c.concept_id is null then 'local concept'
             when c.concept_id is not null and c.concept_id != 0
               and (c.standard_concept is null OR c.standard_concept != 'S') then 'non-standard concept'
        end as error_type
        FROM `/UNITE/LDS/clean/condition_occurrence` co
        JOIN `/UNITE/LDS/clean/manifest_clean` m on m.data_partner_id = co.data_partner_id 
                and (cdm_name = 'OMOP' or cdm_name = 'OMOP (PEDSNET)')
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c   ON (co.condition_concept_id = c.concept_id)
        WHERE -- repeating conditions in CASE to eliminate OK concepts
            co.condition_concept_id is not null and (
                (co.condition_concept_id is not null and c.concept_id = 0)
             OR (co.condition_concept_id is not null and c.concept_id is null)
             OR (c.concept_id is not null and c.concept_id != 0 and (c.standard_concept is null OR c.standard_concept != 'S') )
             )
             and (c.concept_id is null OR c.concept_id not in (2004208004, 2004208005, 2004207791) )
        GROUP BY co.data_partner_id, 
               co.condition_concept_id, co.condition_source_concept_id, co.condition_source_concept_name,
               c.concept_name, c.vocabulary_id, c.concept_code, 
               error_type
),
MEAS_COUNTS as (
        SELECT  m.data_partner_id,  count(*) as ct,  
                m.measurement_concept_id as d_concept_id, m.measurement_source_concept_id as d_source_concept_id,  
                m.measurement_source_concept_name as d_source_concept_name,
                c.concept_name, c.vocabulary_id, c.concept_code,
        CASE -- each includes nc.condition_concept_id is not null b/c it's in the WHERE clause
             when c.concept_id = 0  then 'no matching concept'
             when c.concept_id is null then 'local concept'
             when c.concept_id is not null and c.concept_id != 0
              and (c.standard_concept is null OR c.standard_concept != 'S') then 'non-standard concept'
        end as error_type
        FROM `/UNITE/LDS/clean/measurement` m
               JOIN `/UNITE/LDS/clean/manifest_clean` mani on mani.data_partner_id = m.data_partner_id and (mani.cdm_name = 'OMOP' or mani.cdm_name = 'OMOP (PEDSNET)')
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
          ON (m.measurement_concept_id = c.concept_id)
        WHERE -- repeating conditions in CASE to eliminate OK concepts
            m.measurement_concept_id is not null and (
                (m.measurement_concept_id is not null and c.concept_id = 0)
             OR (m.measurement_concept_id is not null and c.concept_id is null)
             OR (c.concept_id is not null and c.concept_id != 0 and (c.standard_concept is null OR c.standard_concept != 'S') )
            )
            and (c.concept_id is null OR c.concept_id not in (2004208004, 2004208005, 2004207791) )       
        GROUP BY m.data_partner_id, 
               m.measurement_concept_id, m.measurement_source_concept_id, m.measurement_source_concept_name,
               c.concept_name, c.vocabulary_id, c.concept_code, 
               error_type
),
OBS_COUNTS as (
        SELECT  o.data_partner_id,  count(*) as ct,  
                o.observation_concept_id as d_concept_id, o.observation_source_concept_id as d_source_concept_id,  
                o.observation_source_concept_name as d_source_concept_name,
                c.concept_name, c.vocabulary_id, c.concept_code,
       CASE 
             when c.concept_id = 0  then 'no matching concept'
             when c.concept_id is null  then 'local concept'
             when c.concept_id is not null and c.concept_id != 0
               and (c.standard_concept is null OR c.standard_concept != 'S') then 'non-standard concept'
        END as error_type
        FROM `/UNITE/LDS/clean/observation` o
                JOIN `/UNITE/LDS/clean/manifest_clean` m on m.data_partner_id = o.data_partner_id and (m.cdm_name = 'OMOP' or m.cdm_name = 'OMOP (PEDSNET)')
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
          ON (o.observation_concept_id = c.concept_id)
        WHERE -- repeating conditions in CASE to eliminate OK concepts
            o.observation_concept_id is not null and (
                  (o.observation_concept_id is not null and c.concept_id = 0)
               OR (o.observation_concept_id is not null and c.concept_id is null)
               OR (c.concept_id is not null and c.concept_id != 0 and (c.standard_concept is null OR c.standard_concept != 'S') )
            )
            and (c.concept_id is null OR c.concept_id not in (2004208004, 2004208005, 2004207791) )
        GROUP BY o.data_partner_id,
               o.observation_concept_id, o.observation_source_concept_id, o.observation_source_concept_name,
               c.concept_name, c.vocabulary_id, c.concept_code,
               error_type
),

DRUG_COUNTS as (
        SELECT  de.data_partner_id, count(*) as ct,  
                de.drug_concept_id, de.drug_source_concept_id,  de.drug_source_concept_name,
                c.concept_name, c.vocabulary_id, c.concept_code,
        CASE -- each includes nc.condition_concept_id is not null b/c it's in the WHERE clause
                     when c.concept_id = 0  then 'no matching concept'
             when c.concept_id is null then 'local concept'
             when c.concept_id is not null and c.concept_id != 0
               and (c.standard_concept is null OR c.standard_concept != 'S') then 'non-standard concept'
        end as error_type
        FROM `/UNITE/LDS/clean/drug_exposure` de
                JOIN `/UNITE/LDS/clean/manifest_clean` m on m.data_partner_id = de.data_partner_id and (cdm_name = 'OMOP' or cdm_name = 'OMOP (PEDSNET)')
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
          ON (de.drug_concept_id = c.concept_id)
        WHERE -- repeating conditions in CASE to eliminate OK concepts
            de.drug_concept_id is not null and (
                (de.drug_concept_id is not null and c.concept_id = 0)
             OR (de.drug_concept_id is not null and c.concept_id is null)
             OR (c.concept_id is not null and c.concept_id != 0 and (c.standard_concept is null OR c.standard_concept != 'S') )
            )
           and (c.concept_id is null OR c.concept_id not in (2004208004, 2004208005, 2004207791) )
        GROUP BY de.data_partner_id, 
               de.drug_concept_id, de.drug_source_concept_id, de.drug_source_concept_name,
               c.concept_name, c.vocabulary_id, c.concept_code, 
               error_type
),

PROC_COUNTS as (
       SELECT  po.data_partner_id, count(*) as ct,  
                po.procedure_concept_id, po.procedure_source_concept_id,  po.procedure_source_concept_name,
                c.concept_name, c.vocabulary_id, c.concept_code,
        CASE -- each includes nc.condition_concept_id is not null b/c it's in the WHERE clause
                     when c.concept_id = 0  then 'no matching concept'
             when c.concept_id is null then 'local concept'
             when c.concept_id is not null and c.concept_id != 0
               and (c.standard_concept is null OR c.standard_concept != 'S') then 'non-standard concept'
        end as error_type
        FROM `/UNITE/LDS/clean/procedure_occurrence` po
                JOIN `/UNITE/LDS/clean/manifest_clean` m on m.data_partner_id = po.data_partner_id and (cdm_name = 'OMOP' or cdm_name = 'OMOP (PEDSNET)')
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
          ON (po.procedure_concept_id = c.concept_id)
        WHERE -- repeating conditions in CASE to eliminate OK concepts
            po.procedure_concept_id is not null and (
                (po.procedure_concept_id is not null and c.concept_id = 0)
             OR (po.procedure_concept_id is not null and c.concept_id is null)
             OR (c.concept_id is not null and c.concept_id != 0 and (c.standard_concept is null OR c.standard_concept != 'S') )
             )
          and (c.concept_id is null OR c.concept_id not in (2004208004, 2004208005, 2004207791) )
        GROUP BY po.data_partner_id, 
               po.procedure_concept_id, po.procedure_source_concept_id, po.procedure_source_concept_name,
               c.concept_name, c.vocabulary_id, c.concept_code, 
               error_type
),
VISIT_COUNTS as (
        SELECT  v.data_partner_id,  count(*) as ct,  
                v.visit_concept_id, v.visit_source_concept_id,  v.visit_source_concept_name,
                c.concept_name, c.vocabulary_id, c.concept_code,
        CASE -- each includes nc.condition_concept_id is not null b/c it's in the WHERE clause
                     when c.concept_id = 0  then 'no matching concept'
             when c.concept_id is null then 'local concept'
             when c.concept_id is not null and c.concept_id != 0
               and (c.standard_concept is null OR c.standard_concept != 'S') then 'non-standard concept'
        end as error_type
        FROM `/UNITE/LDS/clean/visit_occurrence` v
                JOIN `/UNITE/LDS/clean/manifest_clean` m on m.data_partner_id = v.data_partner_id and (cdm_name = 'OMOP' or cdm_name = 'OMOP (PEDSNET)')
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
          ON (v.visit_concept_id = c.concept_id)
        WHERE -- repeating conditions in CASE to eliminate OK concepts
            v.visit_concept_id is not null and (
                (v.visit_concept_id is not null and c.concept_id = 0)
             OR (v.visit_concept_id is not null and c.concept_id is null)
             OR (c.concept_id is not null and c.concept_id != 0 and (c.standard_concept is null OR c.standard_concept != 'S') )
            )
         and (c.concept_id is null OR c.concept_id not in (2004208004, 2004208005, 2004207791) )
        GROUP BY v.data_partner_id, 
               v.visit_concept_id, v.visit_source_concept_id, v.visit_source_concept_name,
               c.concept_name, c.vocabulary_id, c.concept_code, 
               error_type
)

SELECT co.ct, co.data_partner_id, 'condition_occurrence' as domain, co.error_type, 
        co.d_concept_id, co.d_source_concept_id, co.d_source_concept_name,
        co.concept_name, co.vocabulary_id, co.concept_code,
        RANK() OVER (PARTITION BY co.data_partner_id,  co.error_type ORDER BY ct DESC) as ranked_by_ct
FROM CO_COUNTS co
union all 

SELECT mc.ct, mc.data_partner_id, 'measurement' as domain, mc.error_type, 
        mc.d_concept_id, mc.d_source_concept_id, mc.d_source_concept_name,
        mc.concept_name, mc.vocabulary_id, mc.concept_code,
        RANK() OVER (PARTITION BY mc.data_partner_id, mc.error_type ORDER BY ct DESC) as ranked_by_ct
FROM MEAS_COUNTS mc
union all
SELECT ct, data_partner_id, 'observation' as domain, error_type, 
        d_concept_id, d_source_concept_id, d_source_concept_name,
        concept_name, vocabulary_id, concept_code,
        RANK() OVER (PARTITION BY data_partner_id,  error_type ORDER BY ct DESC) as ranked_by_ct
FROM OBS_COUNTS
union all 

SELECT ct, data_partner_id, 'procedure_occurrence' as domain, error_type, procedure_concept_id as d_concept_id,
        procedure_source_concept_id as d_source_concept_id, procedure_source_concept_name as d_source_concept_name,
        concept_name, vocabulary_id, concept_code,
        RANK() OVER (PARTITION BY data_partner_id, error_type ORDER BY ct DESC) as ranked_by_ct
FROM PROC_COUNTS
union all
SELECT ct, data_partner_id, 'drug_exposure' as domain, error_type, drug_concept_id as d_concept_id, 
        drug_source_concept_id as d_source_concept_id, drug_source_concept_name as d_source_concept_name,
        concept_name, vocabulary_id, concept_code,
        RANK() OVER (PARTITION BY data_partner_id, error_type ORDER BY ct DESC) as ranked_by_ct
FROM DRUG_COUNTS
union all
SELECT ct, data_partner_id, 'visit_occurrence' as domain, error_type, visit_concept_id as d_concept_id,
        visit_source_concept_id as d_source_concept_id, visit_source_concept_name as d_source_concept_name,
        concept_name, vocabulary_id, concept_code,
        RANK() OVER (PARTITION BY data_partner_id, error_type ORDER BY ct DESC) as ranked_by_ct
FROM VISIT_COUNTS
HAVING ranked_by_ct <= 10
