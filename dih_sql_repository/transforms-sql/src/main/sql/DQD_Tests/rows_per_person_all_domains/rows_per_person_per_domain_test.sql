CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/rows_per_person_all_domains/rows_per_person_per_domain_test` AS

WITH care_site_rows_by_data_partner AS
(
        SELECT  a.*,
                pps.number_of_people,
                ROUND((a.row_count / pps.number_of_people), 5) AS rows_per_person
        FROM
        (
            SELECT  data_partner_id,
                    COUNT(*) as row_count,
                    'CARE SITE' AS domain
            FROM `/UNITE/LDS/clean/care_site` 
            GROUP BY data_partner_id
        ) a

        LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site`  pps
        ON a.data_partner_id = pps.data_partner_id
),

condition_era_rows_by_data_partner AS
(
        SELECT  a.*,
                pps.number_of_people,
                ROUND((a.row_count / pps.number_of_people), 5) AS rows_per_person
        FROM
        (
            SELECT  data_partner_id,
                    COUNT(*) as row_count,
                    'CONDITION ERA' AS domain
            FROM `/UNITE/LDS/clean/condition_era` 
            GROUP BY data_partner_id
        ) a

        LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site`  pps
        ON a.data_partner_id = pps.data_partner_id
),

condition_occurrence_rows_by_data_partner AS
(
        SELECT  a.*,
                pps.number_of_people,
                ROUND((a.row_count / pps.number_of_people), 5) AS rows_per_person
        FROM
        (
            SELECT  data_partner_id,
                    COUNT(*) as row_count,
                    'CONDITION OCCURRENCE' AS domain
            FROM `/UNITE/LDS/clean/condition_occurrence` 
            GROUP BY data_partner_id
        ) a

        LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site`  pps
        ON a.data_partner_id = pps.data_partner_id
),


death_rows_by_data_partner AS
(
        SELECT  a.*,
                pps.number_of_people,
                ROUND((a.row_count / pps.number_of_people), 5) AS rows_per_person
        FROM
        (
            SELECT  data_partner_id,
                    COUNT(*) as row_count,
                    'DEATH' AS domain
            FROM `/UNITE/LDS/clean/death` 
            GROUP BY data_partner_id
        ) a

        LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site`  pps
        ON a.data_partner_id = pps.data_partner_id
), 


drug_era_rows_by_data_partner AS
(
        SELECT  a.*,
                pps.number_of_people,
                ROUND((a.row_count / pps.number_of_people), 5) AS rows_per_person
        FROM
        (
            SELECT  data_partner_id,
                    COUNT(*) as row_count,
                    'DRUG ERA' AS domain
            FROM `/UNITE/LDS/clean/drug_era` 
            GROUP BY data_partner_id
        ) a

        LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site`  pps
        ON a.data_partner_id = pps.data_partner_id
),

drug_exposure_rows_by_data_partner AS
(
        SELECT  a.*,
                pps.number_of_people,
                ROUND((a.row_count / pps.number_of_people), 5) AS rows_per_person
        FROM
        (
            SELECT  data_partner_id,
                    COUNT(*) as row_count,
                    'DRUG EXPOSURE' AS domain
            FROM `/UNITE/LDS/clean/drug_exposure` 
            GROUP BY data_partner_id
        ) a

        LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site`  pps
        ON a.data_partner_id = pps.data_partner_id
),

location_rows_by_data_partner AS
(
        SELECT  a.*,
                pps.number_of_people,
                ROUND((a.row_count / pps.number_of_people), 5) AS rows_per_person
        FROM
        (
            SELECT  data_partner_id,
                    COUNT(*) as row_count,
                    'LOCATION' AS domain
            FROM `/UNITE/LDS/clean/location` 
            GROUP BY data_partner_id
        ) a

        LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site`  pps
        ON a.data_partner_id = pps.data_partner_id
),

measurement_rows_by_data_partner AS
(
        SELECT  a.*,
                pps.number_of_people,
                ROUND((a.row_count / pps.number_of_people), 5) AS rows_per_person
        FROM
        (
            SELECT  data_partner_id,
                    COUNT(*) as row_count,
                    'MEASUREMENT' AS domain
            FROM `/UNITE/LDS/clean/measurement` 
            GROUP BY data_partner_id
        ) a

        LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site`  pps
        ON a.data_partner_id = pps.data_partner_id
),

observation_rows_by_data_partner AS
(
        SELECT  a.*,
                pps.number_of_people,
                ROUND((a.row_count / pps.number_of_people), 5) AS rows_per_person
        FROM
        (
            SELECT  data_partner_id,
                    COUNT(*) as row_count,
                    'OBSERVATION' AS domain
            FROM `/UNITE/LDS/clean/observation` 
            GROUP BY data_partner_id
        ) a

        LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site`  pps
        ON a.data_partner_id = pps.data_partner_id
),

observation_period_rows_by_data_partner AS
(
        SELECT  a.*,
                pps.number_of_people,
                ROUND((a.row_count / pps.number_of_people), 5) AS rows_per_person
        FROM
        (
            SELECT  data_partner_id,
                    COUNT(*) as row_count,
                    'OBSERVATION PERIOD' AS domain
            FROM `/UNITE/LDS/clean/observation_period` 
            GROUP BY data_partner_id
        ) a

        LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site`  pps
        ON a.data_partner_id = pps.data_partner_id
),

payer_plan_period_rows_by_data_partner AS
(
        SELECT  a.*,
                pps.number_of_people,
                ROUND((a.row_count / pps.number_of_people), 5) AS rows_per_person
        FROM
        (
            SELECT  data_partner_id,
                    COUNT(*) as row_count,
                    'PAYER PLAN PERIOD' AS domain
            FROM `/UNITE/LDS/clean/payer_plan_period` 
            GROUP BY data_partner_id
        ) a

        LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site`  pps
        ON a.data_partner_id = pps.data_partner_id
),

procedure_occurrence_rows_by_data_partner AS
(
        SELECT  a.*,
                pps.number_of_people,
                ROUND((a.row_count / pps.number_of_people), 5) AS rows_per_person
        FROM
        (
            SELECT  data_partner_id,
                    COUNT(*) as row_count,
                    'PROCEDURE OCCURRENCE' AS domain
            FROM `/UNITE/LDS/clean/procedure_occurrence` 
            GROUP BY data_partner_id
        ) a

        LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site`  pps
        ON a.data_partner_id = pps.data_partner_id
),

provider_rows_by_data_partner AS
(
        SELECT  a.*,
                pps.number_of_people,
                ROUND((a.row_count / pps.number_of_people), 5) AS rows_per_person
        FROM
        (
            SELECT  data_partner_id,
                    COUNT(*) as row_count,
                    'PROVIDER' AS domain
            FROM `/UNITE/LDS/clean/provider` 
            GROUP BY data_partner_id
        ) a

        LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site`  pps
        ON a.data_partner_id = pps.data_partner_id
),

visit_occurrence_rows_by_data_partner AS
(
        SELECT  a.*,
                pps.number_of_people,
                ROUND((a.row_count / pps.number_of_people), 5) AS rows_per_person
        FROM
        (
            SELECT  data_partner_id,
                    COUNT(*) as row_count,
                    'VISIT OCCURRENCE' AS domain
            FROM `/UNITE/LDS/clean/visit_occurrence` 
            GROUP BY data_partner_id
        ) a

        LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site`  pps
        ON a.data_partner_id = pps.data_partner_id
)


SELECT * FROM care_site_rows_by_data_partner
UNION
SELECT * FROM condition_era_rows_by_data_partner
UNION
SELECT * FROM condition_occurrence_rows_by_data_partner
UNION
SELECT * FROM death_rows_by_data_partner
UNION
SELECT * FROM drug_era_rows_by_data_partner
UNION
SELECT * FROM drug_exposure_rows_by_data_partner
UNION
SELECT * FROM location_rows_by_data_partner
UNION
SELECT * FROM measurement_rows_by_data_partner
UNION
SELECT * FROM observation_rows_by_data_partner
UNION
SELECT * FROM observation_period_rows_by_data_partner
UNION
SELECT * FROM payer_plan_period_rows_by_data_partner
UNION
SELECT * FROM procedure_occurrence_rows_by_data_partner
UNION
SELECT * FROM provider_rows_by_data_partner
UNION
SELECT * FROM visit_occurrence_rows_by_data_partner