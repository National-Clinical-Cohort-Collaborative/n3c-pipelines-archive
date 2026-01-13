CREATE TABLE `ri.foundry.main.dataset.5c426fdd-0f71-4d4f-a120-b9a3d68bebdd` AS
    with distinct_deprecated_concepts as (
         select DISTINCT
         domain_concept_id,
         unmapped_source_concept_id,
         source_concept_name, 
         reason,
         source_pkey, 
         source_domain
         FROM `ri.foundry.main.dataset.c647f4a5-1faa-4277-add5-8574c6f3e2e1` 
    ),
    
    deprecated_with_standard_concept as (
        select distinct
        u.*
        , c.standard_concept
        from distinct_deprecated_concepts u
        left join  `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
        on c.concept_id = u.domain_concept_id

    ),
    deprecated_relations as (
        select distinct
        *
        from `ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71` c
        ---'Concept poss_eq from'
        where c.relationship_id in ( 'Concept poss_eq to', 'Concept same_as to', 'Concept replaced by', 'RxNorm is a', 'Inactive possibly_equivalent_to active (SNOMED)', 'Maps to')

    )
    -- with the deprecated concepts user the relationships id listed above to 
    ----find the standard concept so we can rescue the unmapped concepts as standard
    SELECT distinct
    d.*
    , r.*
    FROM deprecated_with_standard_concept d
    left join deprecated_relations r on r.concept_id_1 = d.domain_concept_id
 