from transforms.api import transform_df, Input, Output, configure



@transform_df(
    Output("/UNITE/[RP-4A9E27] DI&H - Data Quality/manifest ontology/dqp_payload_tracker"),
    clean_payloads=Input("/UNITE/LDS/clean/measurement")
)
def my_compute_function(clean_payloads):
    # Get payload for each site that is currently in LDS Clean
    # This dataset should only build after DQD datasets have built, so these are the paylaods in the portal
    df = clean_payloads.selectExpr("data_partner_id", "payload AS dq_portal_payload").distinct()
    return df
