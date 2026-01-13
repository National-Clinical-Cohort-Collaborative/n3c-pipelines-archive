from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from act.anchor import path


@transform(
    processed=Output(path.metadata + 'act_local_code_map'),
    my_input=Input(path.transform + "01 - parsed/metadata/act_standard2local_code_map"),
)
def my_compute_function(my_input, processed):
    processed_df = my_input.dataframe()

    # Convert empty strings to null values
    def blank_as_null(x):
        return F.when(F.col(x) != "", F.col(x)).otherwise(None)
    exprs = [blank_as_null(x).alias(x) for x in processed_df.columns]
    processed_df = processed_df.select(*exprs)

    # Convert all column names to lowercase
    for original_col_name in processed_df.columns:
        processed_df = processed_df.withColumnRenamed(original_col_name, original_col_name.lower())

    processed.write_dataframe(processed_df)
