CREATE TABLE `ri.foundry.main.dataset.359380ec-ca64-43b8-8dcf-051c442148b6` AS
    SELECT 
        VISIT_DIMENSION_ID
        ,enc.patient_num
        ,encounter_num
        ,active_status_cd
        ,start_date
        ,end_date
        ,inout_cd
        ,location_cd
        ,location_path
        ,length_of_stay
        ,enc.update_date
        ,enc.download_date
        ,enc.import_date
        ,enc.sourcesystem_cd
        ,enc.upload_id
        ,'VISIT_DIMENSION' as domain_source
        ,enc.data_partner_id
        ,enc.payload
    FROM `ri.foundry.main.dataset.8466536f-cdf7-4818-8197-24a315bc5b52` enc
    LEFT JOIN `ri.foundry.main.dataset.898923b2-c392-4633-9016-c03748e49dad` p
    on enc.patient_num = p.patient_num and enc.data_partner_id = p.data_partner_id
    where p.patient_num is null and enc.encounter_num is not null 
