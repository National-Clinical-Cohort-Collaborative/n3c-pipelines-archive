CREATE TABLE `ri.foundry.main.dataset.35026849-d033-456d-bb3e-108231edc337` AS

    WITH covid_vaccine_concepts AS (
        SELECT c.concept_id, c.concept_name, c.concept_code, cr.concept_id_2 AS standard_concept_id
        FROM `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
        JOIN `ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71` cr ON c.concept_id = cr.concept_id_1
        WHERE c.vocabulary_id = 'CVX'
        AND cr.relationship_id = 'Maps to'
        AND c.concept_code IN (
            SELECT CVX_Code FROM `ri.foundry.main.dataset.6ceadb44-ee25-4dce-b06c-7b1385b51569`
            WHERE LOWER(Full_Vaccine_Name) LIKE '%covid%'
        )
    ),

    covid_drug_exposure AS (
        SELECT drug_exposure_id, data_partner_id, drug_concept_id,drug_concept_name, person_id, drug_exposure_start_date, drug_exposure_end_date
        FROM `ri.foundry.main.dataset.3feda4dc-5389-4521-ab9e-0361ea5773fd`
        WHERE drug_concept_id IN (SELECT standard_concept_id FROM covid_vaccine_concepts)
    )

    SELECT 
        cd.drug_exposure_id, 
        cd.drug_concept_id,
        p.data_partner_id,
        cd.person_id,
        cd.drug_concept_name,
        cd.drug_exposure_start_date,
        drug_exposure_end_date,
        c.concept_name,
        c.concept_code,
        p.year_of_birth,
        p.gender_concept_name,
        vf.vaccine_txn,
        vf.1_vax_date,
        vf.1_vax_type,
        vf.2_vax_date,
        vf.2_vax_type,
        vf.3_vax_date,
        vf.3_vax_type,
        vf.4_vax_date,
        vf.4_vax_type,
        vf.5_vax_date,
        vf.5_vax_type

    FROM covid_drug_exposure cd
    JOIN `ri.foundry.main.dataset.456df227-b15d-4356-9df6-b113e90eb239` p ON cd.person_id = p.person_id
    JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c ON cd.drug_concept_id = c.concept_id
    LEFT JOIN `ri.foundry.main.dataset.78516907-5a32-441a-88bd-58816e028e91` vf ON cd.person_id = vf.person_id and p.data_partner_id = vf.data_partner_id;

