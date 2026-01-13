CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/conditions/plausible_genders_by_concept_id` 
AS
SELECT  A.*,
        B.total_distinct_patients,
        B.total_rows,
        100 * ROUND(A.number_rows / B.total_rows, 3) AS percent_rows,
        -- fraction of patients having at least one row in this status
        100 * ROUND(A.distinct_patients / B.total_distinct_patients, 3) AS percent_patients


 FROM

(
    SELECT condition_concept_id,
        condition_concept_name,
        implausible_gender,
        data_partner_id,
        plausibleGenderThreshold,
        COUNT(*) AS number_rows,
        COUNT(DISTINCT person_id) as distinct_patients

    FROM
    (
        SELECT *,
        plausible_gender_upper != gender_concept_name AS implausible_gender
        FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/conditions/conditions_domain_concept_level_tests` 
    )

    GROUP BY condition_concept_id,
            condition_concept_name,
            implausible_gender,
            data_partner_id,
            plausibleGenderThreshold

) A



LEFT JOIN

    (   
        -- calculate the totals number of conditions for each concept at each site
        SELECT condition_concept_id, 
            COUNT(*) AS total_rows,
            COUNT (DISTINCT person_id) AS total_distinct_patients,
            data_partner_id
            FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/conditions/conditions_domain_concept_level_tests` 
            GROUP BY condition_concept_id, condition_concept_name,data_partner_id

    ) B

    ON A.condition_concept_id = B.condition_concept_id
    AND A.data_partner_id = B.data_partner_id

