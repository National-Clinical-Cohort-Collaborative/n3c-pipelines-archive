CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/utils/dataPartnersLastRunDate` AS

    with data_partner_rundate as 
    (
        SELECT distinct 
    dp.site_name as site_name,
    COALESCE(dp.data_partner_id, m.data_partner_id) as data_partner_id,
    COALESCE(dp.source_cdm, m.cdm_name) as cdm_name,
    COALESCE(m.run_date, m.contribution_date)  as last_run_date
    FROM `/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/Data Partner IDs - ALL` dp
    join `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` m on dp.data_partner_id = m.data_partner_id
    where dp.data_partner_id is not null 
    )

    -- manifest should provide the last data run date, but some sites do not provide the valid manifest file without the last data_run_date
    -- if the data run date is missing then use the reference file to find the date from the manually created dataset. 
    -- if the run_data is null, use the default run_date from this manually created file. 
    --
    -- most data partner's provide data run date and this date is saved in the manifest file
    -- However, some sites have failed to save this data in the manifest, so we created a default data run date based on the 
    -- dataset submitted date on the sFTP folder. 
    -- try to use the run_date or contribution_date before resorting to use the default_submit_date. 
    -- SH  the site_default_run_date dataset set is save here:
    ---/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/data_partner_default_submit_date - data_partner_default_submit_date_sftp

    select 
    COALESCE(d.long_name, r.site_name) as site_name,
    COALESCE(d.data_partner_id, r.data_partner_id) as data_partner_id,
    COALESCE(m.cdm_name, r.cdm_name ) as cdm_name, 
    COALESCE( r.last_run_date, m.contribution_date, d.last_data_submit_date ) as last_run_date
    from data_partner_rundate r
    join `/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/data_partner_default_submit_date - data_partner_default_submit_date_sftp` d 
    on r.data_partner_id = d.data_partner_id
    full outer join `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` m
    on m.data_partner_id = r.data_partner_id
    where r.data_partner_id is not null