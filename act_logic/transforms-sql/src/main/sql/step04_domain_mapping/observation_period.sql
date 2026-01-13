CREATE TABLE `ri.foundry.main.marketplace-repository-integration.0525347e-6a84-495a-8137-402db6dab0bc_period` AS

-- UNFINISHED DOMAINS ARE COMMENTED OUT

with obs_period AS (
    SELECT 
		  site_patient_num
		, Min(min_date) AS min_date
		, Min(min_datetime) AS min_datetime
		, Max(max_date) AS max_date
		, Max(max_datetime) AS max_datetime
		, 44814724 AS period_type_concept_id
		, data_partner_id
		, payload
	FROM (
		SELECT 
			  site_patient_num
			, data_partner_id
			, payload
            , Min(condition_start_date) AS min_date
            , Min(condition_start_datetime) AS min_datetime
            , Max(COALESCE(condition_end_date, condition_start_date)) AS max_date
            , Max(COALESCE(condition_end_datetime, condition_end_date, condition_start_date)) AS max_datetime
		FROM `ri.foundry.main.dataset.15885ef3-ce89-4ea4-aff0-b9cde9ceba2f`
		GROUP BY site_patient_num, data_partner_id, payload
		UNION 
            SELECT 
				  site_patient_num
				, data_partner_id
				, payload
                , Min(drug_exposure_start_date) AS min_date
                , Min(drug_exposure_start_datetime) AS min_datetime
                , Max(COALESCE(drug_exposure_end_date, drug_exposure_start_date)) AS max_date
                , Max(COALESCE(drug_exposure_end_datetime, drug_exposure_end_date, drug_exposure_start_date)) AS max_datetime
            FROM `ri.foundry.main.dataset.42ffcb6c-bcf0-4485-af76-bb7b4c865a1b`
		    GROUP BY site_patient_num, data_partner_id, payload
		UNION 
            SELECT 
				  site_patient_num
				, data_partner_id
				, payload
                , Min(procedure_date) AS min_date
                , Min(procedure_datetime) AS min_datetime
                , Max(procedure_date) AS max_date
                , Max(procedure_datetime) AS max_datetime
            FROM `ri.foundry.main.dataset.67cce63c-d550-416b-aede-614103839099`
	        GROUP BY site_patient_num, data_partner_id, payload
	    UNION
            SELECT 
				  site_patient_num
				, data_partner_id
				, payload
                , Min(observation_date) AS min_date
                , Min(observation_datetime) AS min_datetime
                , Max(observation_date) AS max_date
                , Max(observation_datetime) AS max_datetime
            FROM `ri.foundry.main.dataset.39bcd55d-24d4-49c0-bc32-2d02c8f7a4bb`
		    GROUP BY site_patient_num, data_partner_id, payload
		UNION 
            SELECT 
				  site_patient_num
				, data_partner_id
				, payload
                , Min(measurement_date) AS min_date
                , Min(measurement_datetime) AS min_datetime
                , Max(measurement_date) AS max_date
                , Max(measurement_datetime) AS max_datetime
            FROM `ri.foundry.main.dataset.61b4488b-3eb8-40c8-8fb4-028b2cf336d0`
		    GROUP BY site_patient_num, data_partner_id, payload
		UNION 
		    SELECT 
				  site_patient_num
				, data_partner_id
				, payload
			    , Min(device_exposure_start_date) AS min_date
			    , Min(device_exposure_start_datetime) AS min_datetime
				, Max(COALESCE(device_exposure_end_date, device_exposure_start_date)) AS max_date
				, Max(COALESCE(device_exposure_end_datetime, device_exposure_end_date, device_exposure_start_date)) AS max_datetime
			FROM `ri.foundry.main.dataset.c7178829-a2f6-4af4-a236-642273ec26de`
		    GROUP BY site_patient_num, data_partner_id, payload
		UNION 
			SELECT 
				  site_patient_num
				, data_partner_id
				, payload
				, Min(visit_start_date) AS min_date
				, Min(visit_start_datetime) AS min_datetime
				, Max(COALESCE(visit_end_date, visit_start_date)) AS max_date
				, Max(COALESCE(visit_end_datetime, visit_end_date, visit_start_date)) AS max_datetime
			FROM `ri.foundry.main.dataset.3069c85a-1f5e-4046-bd47-8ebf8a9acd55`
			GROUP BY site_patient_num, data_partner_id, payload
		UNION 
			SELECT
				  site_patient_num
				, data_partner_id
				, payload
				, Min(death_date) AS min_date
				, Min(death_datetime) AS min_datetime
				, Max(death_date) AS max_date
				, Max(death_datetime) AS max_datetime
			FROM `ri.foundry.main.dataset.97386241-9e4d-43cb-a7f8-c34b2c636234`
			GROUP BY site_patient_num, data_partner_id, payload
	) t
	GROUP BY t.site_patient_num, t.data_partner_id, t.payload
)

SELECT
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247  as observation_period_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
	, hashed_id
	, site_patient_num
	, min_date as observation_period_start_date
	, min_datetime as observation_period_start_datetime
	, max_date as observation_period_end_date
	, max_datetime as observation_period_end_datetime
	, period_type_concept_id
	, data_partner_id
	, payload
FROM (
    SELECT
    	  *
    	, md5(concat_ws(
              ';'
			, COALESCE(site_patient_num, ' ')
			, COALESCE(min_date, ' ')
			, COALESCE(min_datetime, ' ')
			, COALESCE(max_date, ' ')
			, COALESCE(max_datetime, ' ')
			, COALESCE(period_type_concept_id, ' ')
        )) as hashed_id
    FROM obs_period
)