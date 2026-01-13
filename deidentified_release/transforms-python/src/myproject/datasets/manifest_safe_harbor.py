from transforms.api import transform_df, Input, Output, Markings
from myproject.datasets import utils


@transform_df(
    Output("ri.foundry.main.dataset.18c4a3ad-37c7-44f6-be2f-02432b36ea8b"),
    my_input=Input("ri.foundry.main.dataset.d87628fb-b9cd-449d-bddc-b463c272c9da", stop_propagating=Markings([
                                                                       "2f2690b6-e4be-4873-852d-c2f1a6a61740",
                                                                       "6c04089d-ae03-4ae5-b19b-a3912df4552e",
                                                                       "032e4f9b-276a-48dc-9fb8-0557136ffe47",
                                                                   ], on_branches=utils.release_branches)),
)
def my_compute_function(my_input):

    df = my_input

    df = df.withColumnRenamed("site_id", "data_partner_id")
    df = df.filter(df["released"] == True)

    # drop the old site_id from the SH version.
    df = df.drop("old_site_id")
    df = df.drop("n3c_site_id")

    df = df.select(
        "data_partner_id",
        "cdm_name",
        "cdm_version",
        "run_date",
        "contribution_date",
        "n3c_phenotype_yn",
        "n3c_phenotype_version",
        "vocabulary_version",
        "mapped_version",
        "datasetbuildversion",
        "releasenote",
        "shift_date_yn",
        "max_num_shift_days",
        "site_to_site_linkage",
        "cms_linkage",
        "mortality_linkage",
        "viral_variants_linkage"
    )

    return df
