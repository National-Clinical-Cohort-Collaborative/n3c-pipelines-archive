-- This query finds when the reported source_value has not been mapped
-- to the correct OMOP standard concept id.  This is done by several hops.
-- 1: we look up the OMOP concept id for the source value in the CONCEPT table. 
-- 2: use the CONCEPT_RELATIONSHIP table to find which Standard concept(s) this "Maps to"
-- 3: since a single non-standard concept id can map to multiple standard concept ids, we aggregate these into a list
-- 4: finally we look if the original concept id recorded in the table is in the set of
-- mapped to concept ids - if not then we know there is an error

CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/concept_mapping/concept_measurement_domain` TBLPROPERTIES (foundry_transform_profiles = 'NUM_EXECUTORS_8, SHUFFLE_PARTITIONS_LARGE') AS
    SELECT * FROM 
    (
        SELECT 
              d.person_id
            , d.measurement_id as id
            , d.measurement_source_value as source_value
            , d.measurement_concept_id as reported_concept_id
            , collect_list(d.concept_id_2) OVER (PARTITION BY d.measurement_id) as mapped_standard_concept_ids 
            , d.data_partner_id
            , 'Measurement' as domain
        FROM
        (
            SELECT c.*, rel.concept_id_2 FROM
                (
                    SELECT 
                          dom.* 
                        , concept.concept_id as source_mapped_concept_id
                    FROM `/UNITE/LDS/clean/measurement` dom
                        -- JOIN `/UNITE/OMOP Vocabularies/concept` concept
                        JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772`  concept
                        ON dom.measurement_source_value = concept.concept_code
                        WHERE concept.domain_id = 'Measurement'
                ) c
--                JOIN `/UNITE/OMOP Vocabularies/concept_relationship` rel
                JOIN `ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71` rel
                ON c.source_mapped_concept_id = rel.concept_id_1
                WHERE rel.relationship_id = 'Maps to'        
        ) d ) e
    WHERE NOT array_contains(e.mapped_standard_concept_ids, e.reported_concept_id)