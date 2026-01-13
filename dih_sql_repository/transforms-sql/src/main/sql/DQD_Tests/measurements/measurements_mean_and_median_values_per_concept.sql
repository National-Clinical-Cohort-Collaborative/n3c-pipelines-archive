CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/measurements_mean_and_median_values_per_concept` TBLPROPERTIES (foundry_transform_profiles = 'NUM_EXECUTORS_16') AS
    
/* want to find places where the sites have very different medians or means 
Consider a calculation that isolates sites that are far from others */

SELECT      A.data_partner_id,
            A.measurement_concept_id,
            A.measurement_concept_name,
            A.unit_concept_id,
            A.unit_concept_name,
            ROUND( (A.median / B.median_all_sites), 3) AS ratio_of_medians,
            A.first_quartile,
            A.median,
            A.third_quartile,
            A.total_rows,
            B.first_quartile_all_sites,
            B.median_all_sites,
            B.third_quartile_all_sites,
            B.total_rows_all_sites


            FROM


            (

                /* distribution of lab values inside */

                SELECT  data_partner_id,
                        measurement_concept_id,
                        measurement_concept_name,
                        unit_concept_id,
                        unit_concept_name,
                        PERCENTILE(value_as_number, 0.25) AS first_quartile,
                        PERCENTILE(value_as_number, 0.5) AS median,
                        PERCENTILE(value_as_number, 0.75) AS third_quartile,
                        COUNT(*) as total_rows
                
                FROM `/UNITE/LDS/clean/measurement`
                WHERE value_as_number IS NOT NULL
                GROUP BY    data_partner_id,
                            measurement_concept_id,
                            measurement_concept_name,
                            unit_concept_id,
                            unit_concept_name
            ) A

            LEFT JOIN

            (

                /* Distribution of each measure : Unit combination across ALL sites */

                SELECT  
                        measurement_concept_id,
                        measurement_concept_name,
                        unit_concept_id,
                        unit_concept_name,
                        PERCENTILE(value_as_number, 0.25) AS first_quartile_all_sites,
                        PERCENTILE(value_as_number, 0.5) AS median_all_sites,
                        PERCENTILE(value_as_number, 0.75) AS third_quartile_all_sites,
                        COUNT(*) as total_rows_all_sites
                
                FROM `/UNITE/LDS/clean/measurement`
                WHERE value_as_number IS NOT NULL
                GROUP BY    
                            measurement_concept_id,
                            measurement_concept_name,
                            unit_concept_id,
                            unit_concept_name


            ) B

            ON A.measurement_concept_id = B.measurement_concept_id
            AND A.unit_concept_id = B.unit_concept_id