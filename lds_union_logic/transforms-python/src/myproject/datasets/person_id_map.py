from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F
from myproject.datasets.utils import get_person_id_map_inputs, perform_union

inputs = {
    #**get_person_id_map_inputs("transform/06 - id generation/person", "TRINETX"),
    **get_person_id_map_inputs("transform/05 - global id generation/person", "OMOP"),
    #**get_person_id_map_inputs("transform/06 - global id generation/person", "PEDSNET"),
    #**get_person_id_map_inputs("transform/06 - id generation/person", "ACT"),
    **get_person_id_map_inputs("transform/06 - id generation/person", "PCORNET")
}

col_name_mapping = {
    "person_id": "n3c_person_id",
    "site_patient_id": "site_person_id",  # TriNetX
    "site_patid": "site_person_id",  # PCORnet
    "site_patient_num": "site_person_id"  # ACT
}


@transform_df(
    Output("/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/unioned_person_id_map"),
    **inputs
)
def my_compute_function(ctx, **inputs):
    dfs_to_union = {}
    for alias, input_df in inputs.items():
        df = input_df

        # Remap columns to harmonized names
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.lower())
            if col_name in col_name_mapping:
                new_name = col_name_mapping[col_name]
                df = df.withColumnRenamed(col_name, new_name)

        df = df \
            .withColumn("source_cdm", F.lit(alias.split('_')[0])) \
            .withColumn("site_person_id", F.col("site_person_id").cast('string')) \
            .withColumn("n3c_person_id", F.col("n3c_person_id").cast('string'))

        dfs_to_union[alias] = df

    return perform_union(ctx, "person_id_map", **dfs_to_union)
