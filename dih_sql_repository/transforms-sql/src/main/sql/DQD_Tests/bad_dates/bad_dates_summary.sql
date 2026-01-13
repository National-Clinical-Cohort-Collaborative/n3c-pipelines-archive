CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/bad_dates_summary` TBLPROPERTIES (foundry_transform_profiles = 'NUM_EXECUTORS_16, EXECUTOR_MEMORY_SMALL') AS
-- narrows down the data in bad_dates_single and bad_dates_start_end to just
-- data_partner_id, domain and a count.


  select data_partner_id, domain, early + future  as bad_ct, ct, 
    early, future, is_null,
    early / ct * 100 as early_pct,
    future / ct * 100 as future_pct,
    (early + future)  / ct * 100 as bad_pct,
    is_null / ct * 100 as null_pct
  from  `ri.foundry.main.dataset.048f3439-0417-48bb-b7f1-b433b54cef5b` 
  union all 
  select data_partner_id, domain, early + future as bad_ct, ct, 
    early, future, is_null,
    early / ct * 100 as early_pct, 
    future / ct * 100 as future_pct, 
    (early+ future) / ct * 100 as bad_pct,
    is_null /ct * 100 as null_pct
  from `ri.foundry.main.dataset.a8038ec5-a5a7-489a-84f9-0a1318d8baea`
