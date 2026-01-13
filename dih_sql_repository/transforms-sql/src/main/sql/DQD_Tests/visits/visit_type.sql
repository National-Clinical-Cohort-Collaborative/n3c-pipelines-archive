CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/visits/visit_type` AS

-- NOTE: Concepts were referenced from N3C healthcare utilization report q3 2022
  WITH visit_counts AS

    (
    SELECT data_partner_id,
          CASE
            WHEN visit_concept_id IN ( 32254, 38004282, 38004290, 38004515,
                                        262, 32760, 8971, 581384,
                                        8717, 9201, 581379, 38004276,
                                        38004281, 38004283, 38004284, 581383,
                                        38004275, 38004279, 38004280, 32037,
                                        38004288 )
          THEN 'Inpatient'
            WHEN visit_concept_id IN ( 8716, 8782, 8947, 8977,
                                        32253, 38003820, 38004220, 38004235,
                                        38004239, 38004256, 38004263, 38004264,
                                        38004515, 38004696, 5084, 8761,
                                        8969, 32693, 38004216, 38004222,
                                        38004223, 38004254, 8941, 9202,
                                        32261, 581479, 38003620, 38004208,
                                        38004211, 38004227, 38004243, 38004244,
                                        38004246, 38004252, 38004257, 8756,
                                        8883, 8966, 38003821, 38004209,
                                        38004215, 38004226, 38004228, 38004236,
                                        38004237, 38004242, 38004247, 38004249,
                                        38004261, 38004262, 38004266, 38004267,
                                        38004453, 38004693, 8584, 8960,
                                        38004225, 38004234, 38004241, 38004245,
                                        38004248, 38004250, 38004253, 38004258,
                                        38004260, 8858, 8949, 8968,
                                        581477, 38004207, 38004210, 38004213,
                                        38004217, 38004218, 38004229, 38004240,
                                        38004251, 38004259, 38004268, 38004269,
                                        38004677, 38004702, 8905, 8974,
                                        581380, 38004233, 38004238, 38004526 ) THEN
            'Outpatient/Ambulatory'
            WHEN visit_concept_id IN ( 8870, 9203, 581381 ) THEN 'ED'
            ELSE 'No matching concept'
          end      AS visit_type,
          Count(*) AS visit_count
    FROM   `/UNITE/LDS/clean/visit_occurrence`
    GROUP  BY data_partner_id,
              visit_type  
    ORDER  BY data_partner_id,
              visit_type  


    ),


    total_counts_by_site AS
    (
        SELECT data_partner_id, count(*) AS total_site_visit_count
        FROM `/UNITE/LDS/clean/visit_occurrence`
        GROUP BY data_partner_id
    )


    SELECT  v.data_partner_id, 
            v.visit_type,
            v.visit_count, 
            t.total_site_visit_count,
            ROUND((visit_count* 100 / total_site_visit_count),2) AS percent_visits

    FROM visit_counts v
    LEFT JOIN total_counts_by_site t
    ON v.data_partner_id = t.data_partner_id
