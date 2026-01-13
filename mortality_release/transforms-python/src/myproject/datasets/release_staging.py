from transforms.api import transform_df, Input, Output


@transform_df(
    Output("ri.foundry.main.dataset.63e13a77-c9da-49a6-8366-afdf6e49bb06"),
    mortality_df=Input("ri.foundry.main.dataset.b3bbb7ad-9ed1-456b-886f-2b568b93e402"),
    lds_manifest=Input("ri.foundry.main.dataset.b1e99f7f-5dcd-4503-985a-bbb28edc8f6f")
)
def compute_function(mortality_df, lds_manifest):
    # Filter to released sites only
    mortality_filtered = mortality_df \
        .join(lds_manifest.select("data_partner_id"),
              on='data_partner_id',
              how='inner')

    return mortality_filtered.select(
        "person_id",
        "date_of_birth",
        "date_of_death",
        "death_verification",
        "state",
        "datasource_type",
        "data_partner_id"
    )
