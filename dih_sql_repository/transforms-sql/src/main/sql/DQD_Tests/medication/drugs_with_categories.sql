CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/vaccine/drugs_with_categories` AS
    
   WITH simplified_drug_concepts AS (
        SELECT 
            ca.descendant_concept_id,
            anc_con.concept_id as anc_con_id,
            anc_con.concept_name as anc_con_name
        FROM `ri.foundry.main.dataset.c5e0521a-147e-4608-b71e-8f53bcdbe03c` ca
        JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` anc_con ON ca.ancestor_concept_id = anc_con.concept_id
        WHERE lower(anc_con.vocabulary_id) = 'rxnorm' 
            AND anc_con.concept_class_id = 'Ingredient'
    ),
    filtered_drug_exposure AS (
        SELECT 
            de.drug_exposure_id, 
            de.data_partner_id, 
            de.person_id,
            sdc.anc_con_id as drug_concept_id,
            sdc.anc_con_name as drug_concept_name,
            de.drug_exposure_start_date,
            de.drug_exposure_end_date
        FROM `ri.foundry.main.dataset.3feda4dc-5389-4521-ab9e-0361ea5773fd` de
        JOIN simplified_drug_concepts sdc ON de.drug_concept_id = sdc.descendant_concept_id
        WHERE de.drug_concept_id IN (
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
            AND lower(concept_name) NOT LIKE '%antigen%'
            AND lower(concept_name) NOT LIKE '%vaccine%' -- newly added to exclude vaccines/antigens not captured by 'CVX'
        )
    )
    SELECT 
        fde.drug_exposure_id, 
        fde.data_partner_id,
        fde.person_id,
        fde.drug_concept_id,
        fde.drug_concept_name,
        fde.drug_exposure_start_date,
        fde.drug_exposure_end_date,
        p.year_of_birth,
        p.gender_concept_name,
        p.race_concept_name
    FROM filtered_drug_exposure fde
    JOIN `ri.foundry.main.dataset.456df227-b15d-4356-9df6-b113e90eb239` p ON fde.person_id = p.person_id;