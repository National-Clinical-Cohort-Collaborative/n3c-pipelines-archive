from myproject.config import sites, input_path, output_path
from transforms.api import transform_df, Input, Output
from transforms.verbs.dataframes import union_many

inputs = {
    site: Input(f"{input_path}/Site {site}/transform/02 - prepared - 01/measurement")
    for site in sites
}


@transform_df(
    Output(f"{output_path}/union_prepared_measurement"),
    **inputs,
)
def compute(**inputs):
    selected_columns = [
        "data_partner_id",
        "source_standard_concept",
        "target_domain_id",
        "target_standard_concept",
    ]

    # Select the specified columns from each input DataFrame
    dfs = [df.select(*selected_columns) for df in inputs.values()]

    # Perform a union of all the DataFrames
    unioned_df = union_many(*dfs)

    return unioned_df
