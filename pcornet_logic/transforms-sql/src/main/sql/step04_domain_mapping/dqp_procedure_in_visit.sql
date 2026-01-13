CREATE TABLE `ri.foundry.main.dataset.09a4bde5-4657-46d2-a95c-df8ab07df703` AS
with procedure_temp as (
        SELECT distinct
        encounterid as site_encounterid,
        patid AS site_patid,
        cast(COALESCE(xw.target_concept_id, 0) as int) as visit_concept_id,
        cast(pr.admit_date as date) as visit_start_date,
        cast(pr.admit_date as timestamp) as visit_start_datetime, --** MB: admit_date is just a date col, no time info
        cast(pr.admit_date as date) as visit_end_date,
        cast(pr.admit_date as timestamp) as visit_end_datetime,
        -- confirmed this issue:
        ---Stephanie Hong 6/19/2020 -32035 -default to 32035 "Visit derived from EHR encounter record.
        ---case when enc.enc_type in ('ED', 'AV', 'IP', 'EI') then 38000251  -- need to check this with Charles / missing info
        ---when enc.enc_type in ('OT', 'OS', 'OA') then 38000269
        ---else 0 end AS VISIT_TYPE_CONCEPT_ID,  --check with SMEs
        case when ( vsrc.TARGET_CONCEPT_ID != 0 and vsrc.TARGET_CONCEPT_ID is not null) then vsrc.TARGET_CONCEPT_ID
            else 32035
            end as visit_type_concept_id,
        --32035 as visit_type_concept_id, ---- where did the record came from / need clarification from SME
        -- MB: the two below fields get filled in during step 06
        CAST(pr.providerid AS long) as provider_id,
        CAST(null AS long) as care_site_id,
        cast(pr.px as string) as visit_source_value,
        cast(xw.source_concept_id as int) as visit_source_concept_id,  
        cast(vsrc.TARGET_CONCEPT_ID as int) AS admitting_source_concept_id,
        cast(pr.px_source as string) AS admitting_source_value,
        cast(null as int) AS discharge_to_concept_id,
        cast(null as string) AS discharge_to_source_value,
        cast(null as long) AS preceding_visit_occurrence_id, 
        'PROCEDURES' as domain_source,
        data_partner_id,
        payload
    FROM `ri.foundry.main.dataset.da113963-4e92-42a2-9477-2591001cfcc1` pr
        INNER JOIN `ri.foundry.main.dataset.05c59431-1c54-4ef8-a475-b3772d8e830f` xw 
            ON xw.CDM_TBL = 'PROCEDURES' AND xw.target_domain_id = 'Visit'
            AND pr.px = xw.src_code
            AND xw.src_code_type = pr.px_type
        LEFT JOIN `ri.foundry.main.dataset.9cd6e457-ddf3-416a-9bad-ffc36b30fcf3` vsrc 
            ON vsrc.CDM_TBL = 'PROCEDURES' 
            AND vsrc.CDM_SOURCE = 'PCORnet' 
            AND vsrc.CDM_TBL_COLUMN_NAME = 'PX_SOURCE'
            AND vsrc.SRC_CODE = pr.px_source 
        where xw.target_domain_id = 'Visit'

),
procedure as (
    select data_partner_id, count(*) as procedure_with_visit_cnt
    from procedure_temp
    group by data_partner_id
),
visit as (
    select data_partner_id, count(*) as visit_incoming_cnt 
    from `ri.foundry.main.dataset.419a0f18-8aa3-4459-9df8-c5b8b43f09b4`  
    where domain_source = 'PROCEDURES'
    group by data_partner_id
)
select p.*,v.visit_incoming_cnt from procedure p
left join visit v
on p.data_partner_id = v.data_partner_id