CREATE TABLE `ri.foundry.main.dataset.56d8e5ba-528d-4575-b403-e77deddea345` AS
    WITH covid_drug_exposure AS (

    SELECT 
        drug_exposure_id, 
        data_partner_id, 
        drug_concept_id, 
        drug_concept_name, 
        person_id, 
        drug_exposure_start_date, 
        drug_exposure_end_date
    FROM `ri.foundry.main.dataset.3feda4dc-5389-4521-ab9e-0361ea5773fd`
    WHERE drug_concept_id IN (
        SELECT concept_id 
        FROM `ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6` 
        WHERE concept_set_name IN ('paxlovid or nirmatrelvir', 'Remdesivir') 
        AND is_most_recent_version IS TRUE
    )
)
    SELECT 
        cd.drug_exposure_id, 
        cd.data_partner_id,
        cd.drug_concept_id,
        cd.person_id,
        cd.drug_concept_name,
        cd.drug_exposure_start_date,
        cd.drug_exposure_end_date,
        p.year_of_birth,
        p.gender_concept_name,
        p.race_concept_name
    FROM covid_drug_exposure cd
    JOIN `ri.foundry.main.dataset.456df227-b15d-4356-9df6-b113e90eb239` p ON cd.person_id = p.person_id