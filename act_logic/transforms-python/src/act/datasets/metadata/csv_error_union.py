from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from transforms.verbs import dataframes as D
from act.anchor import path

cached_tables = ["note", "note_nlp"]
metadata_tables = ["act_standard2local_code_map", "control_map", "data_counts", "manifest", "n3c_vocab_map"]
optional_tables = ["concept_dimension"]
required_tables = ["observation_fact", "patient_dimension", "visit_dimension"]

all_tables = cached_tables + metadata_tables + optional_tables + required_tables
all_inputs = {table: Input(path.transform + "01 - parsed/errors/" + table) for table in all_tables}


@transform_df(
    Output(path.metadata + "csv_error_union"),
    **all_inputs
)
def compute(**input_dfs):
    selected_dfs = list(df.withColumn("source_table", F.lit(name)) for name, df in input_dfs.items())
    unioned_df = D.union_many(*selected_dfs)
    return unioned_df.select("source_table", "row_number", "error_type", "error_details")
