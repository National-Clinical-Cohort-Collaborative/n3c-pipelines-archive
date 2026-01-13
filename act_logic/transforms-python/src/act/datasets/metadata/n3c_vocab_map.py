from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from act.anchor import path


@transform(
    processed=Output(path.metadata + "n3c_vocab_map"),
    my_input=Input(path.transform + "01 - parsed/metadata/n3c_vocab_map"),
)
def my_compute_function(ctx, my_input, processed):
    processed_df = my_input.dataframe()

    # Lowercase column names
    processed_df = processed_df.select(*[F.col(col).alias(col.lower()) for col in processed_df.columns])
    processed.write_dataframe(processed_df)
