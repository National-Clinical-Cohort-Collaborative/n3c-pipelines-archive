CREATE TABLE `ri.foundry.main.dataset.5482e920-5458-46ab-a051-aec45175a951` AS
    WITH filtered_drug_exposure AS (
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
            FROM `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772`
            WHERE domain_id = 'Drug'
            AND concept_id NOT IN (
                SELECT concept_id 
                FROM `ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6` WHERE codeset_id IN ('412196267', '719693192') 
                AND is_most_recent_version IS TRUE
            )
            AND concept_id NOT IN (
                SELECT cr.concept_id_2
                FROM `ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71` cr
                JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c ON cr.concept_id_1 = c.concept_id
                WHERE c.vocabulary_id = 'CVX'
                AND cr.relationship_id = 'Maps to'
            )
        )
    )

    SELECT 
        de.drug_exposure_id, 
        de.data_partner_id,
        de.drug_concept_id,
        de.person_id,
        de.drug_concept_name,
        de.drug_exposure_start_date,
        de.drug_exposure_end_date,
        p.year_of_birth,
        p.gender_concept_name,
        p.race_concept_name
    FROM filtered_drug_exposure de
    JOIN `ri.foundry.main.dataset.456df227-b15d-4356-9df6-b113e90eb239` p ON de.person_id = p.person_id;