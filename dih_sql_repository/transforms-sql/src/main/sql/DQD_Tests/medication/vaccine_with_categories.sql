CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/vaccine/vaccine_with_categories` AS  
    WITH vaccine_concepts AS (
        SELECT c.concept_id, c.concept_name, c.concept_code, cr.concept_id_2 AS standard_concept_id
        FROM `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
        JOIN `ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71` cr ON c.concept_id = cr.concept_id_1
        WHERE c.vocabulary_id = 'CVX'
        AND cr.relationship_id = 'Maps to'
        AND c.concept_code IN (
           SELECT CVX_Code FROM `ri.foundry.main.dataset.6ceadb44-ee25-4dce-b06c-7b1385b51569`
           --WHERE VaccineStatus = 'Active'
        )
    ),

    drug_exposure AS (
        SELECT drug_exposure_id, data_partner_id, drug_concept_id, drug_concept_name, person_id, drug_exposure_start_date, drug_exposure_end_date
        FROM `ri.foundry.main.dataset.3feda4dc-5389-4521-ab9e-0361ea5773fd`
        WHERE drug_concept_id IN (SELECT standard_concept_id FROM vaccine_concepts)
    )

    SELECT 
        d.drug_exposure_id, 
        d.drug_concept_id,
        p.data_partner_id,
        d.person_id,
        d.drug_concept_name,
        d.drug_exposure_start_date,
        drug_exposure_end_date,
        c.concept_name,
        c.concept_code,
        p.year_of_birth,
        p.gender_concept_name,
        CASE -- newly added section to categorize the vaccines
            WHEN LOWER(c.concept_name) LIKE '%covid-19%'
                OR LOWER(c.concept_name) LIKE '%coronavirus%' THEN 'COVID-19'
            WHEN LOWER(c.concept_name) LIKE '%influenza%' THEN 'Influenza'
            WHEN LOWER(c.concept_name) LIKE '%diphtheria%' 
                OR LOWER(c.concept_name) LIKE '%tetanus%' 
                OR LOWER(c.concept_name) LIKE '%pertussis%' 
                OR LOWER(c.concept_name) LIKE '%td%' 
                OR LOWER(c.concept_name) LIKE '%dtap%' THEN 'DTaP'
            WHEN LOWER(c.concept_name) LIKE '%hepatitis%' THEN 'Hepatitis'
            WHEN LOWER(c.concept_name) LIKE '%haemophilus influenzae%' 
                OR LOWER(c.concept_name) LIKE '%hib%' THEN 'Hib'
            WHEN LOWER(c.concept_name) LIKE '%human papillomavirus%'
                OR LOWER(c.concept_name) LIKE '%papilloma%' 
                OR LOWER(c.concept_name) LIKE '%hpv%' THEN 'HPV'
            WHEN LOWER(c.concept_name) LIKE '%measles%' 
                OR LOWER(c.concept_name) LIKE '%mumps%' 
                OR LOWER(c.concept_name) LIKE '%rubella%' 
                OR LOWER(c.concept_name) LIKE '%mmr%' THEN 'MMR'
            WHEN LOWER(c.concept_name) LIKE '%meningococcal%' THEN 'Meningococcal'
            WHEN LOWER(c.concept_name) LIKE '%pneumococcal%' THEN 'Pneumococcal'
            WHEN LOWER(c.concept_name) LIKE '%polio%' THEN 'Polio'
            WHEN LOWER(c.concept_name) LIKE '%rabies%' THEN 'Rabies'
            WHEN LOWER(c.concept_name) LIKE '%rotavirus%' THEN 'Rotavirus'
            WHEN LOWER(c.concept_name) LIKE '%varicella%' THEN 'Varicella'
            WHEN LOWER(c.concept_name) LIKE '%yellow fever%' THEN 'Yellow Fever'
            WHEN LOWER(c.concept_name) LIKE '%cholera%' THEN 'Cholera'
            WHEN LOWER(c.concept_name) LIKE '%dengue%' THEN 'Dengue'
            WHEN LOWER(c.concept_name) LIKE '%japanese encephalitis%' THEN 'Japanese Encephalitis'
            WHEN LOWER(c.concept_name) LIKE '%tick-borne encephalitis%' THEN 'Tick-borne Encephalitis'
            WHEN LOWER(c.concept_name) LIKE '%typhoid%' THEN 'Typhoid'
            WHEN LOWER(c.concept_name) LIKE '%malaria%' THEN 'Malaria'
            WHEN LOWER(c.concept_name) LIKE '%zoster%' THEN 'Shingles'
            WHEN LOWER(c.concept_name) LIKE '%smallpox%' THEN 'Smallpox'
            WHEN LOWER(c.concept_name) LIKE '%respiratory syncytial virus%' THEN 'RSV'
            WHEN LOWER(c.concept_name) LIKE '%anthrax%' THEN 'Anthrax'
            WHEN LOWER(c.concept_name) LIKE '%bacillus%' 
                OR LOWER(c.concept_name) LIKE '%tuberculin%' THEN 'Tuberculosis'
            ELSE 'Other'
        END AS vaccine_category
    FROM drug_exposure d
    JOIN `ri.foundry.main.dataset.456df227-b15d-4356-9df6-b113e90eb239` p ON d.person_id = p.person_id
    JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c ON d.drug_concept_id = c.concept_id
    WHERE lower(c.concept_name) NOT LIKE '%test%'
    AND lower(c.concept_name) NOT LIKE '%globulin%';
