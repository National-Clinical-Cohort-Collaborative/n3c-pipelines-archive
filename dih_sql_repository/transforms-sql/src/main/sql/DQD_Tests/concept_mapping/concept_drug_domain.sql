
CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/concept_mapping/concept_drug_domain` TBLPROPERTIES (foundry_transform_profiles = 'SHUFFLE_PARTITIONS_LARGE')
    AS SELECT * FROM 
    (
        SELECT 
              d.person_id
            , d.drug_exposure_id as id
            , d.drug_source_value as source_value
            , d.drug_concept_id as reported_concept_id
            , collect_list(d.concept_id_2) OVER (PARTITION BY d.drug_exposure_id) as mapped_standard_concept_ids 
            , d.data_partner_id
            , 'Drug' as domain
        FROM
        (
            SELECT c.*, rel.concept_id_2 FROM
                (
                    SELECT 
                          dom.* 
                        , concept.concept_id as source_mapped_concept_id
                    FROM `/UNITE/LDS/clean/drug_exposure` dom
                        -- JOIN `/UNITE/OMOP Vocabularies/concept` concept
                        JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` concept
                        ON dom.drug_source_value = concept.concept_code
                        WHERE concept.domain_id = 'Drug'
                ) c
                --JOIN `/UNITE/OMOP Vocabularies/concept_relationship` rel
                JOIN `ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71` rel
                ON c.source_mapped_concept_id = rel.concept_id_1
                WHERE rel.relationship_id = 'Maps to'        
        ) d ) e
    WHERE NOT array_contains(e.mapped_standard_concept_ids, e.reported_concept_id)