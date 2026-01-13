from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F
from act.anchor import path


@transform_df(
    Output(path.metadata + "vocab_map_check"),
    vocab_map=Input(path.metadata + "n3c_vocab_map"),
    observation_fact=Input(path.transform + "03 - prepared/observation_fact")
)
def my_compute_function(vocab_map, observation_fact):
    # Flag any vocabulary codes that the site used in its observation_fact table but did not list in its N3C_VOCAB_MAP

    vocab_map = vocab_map.withColumn("local_prefix", F.regexp_replace(F.col("local_prefix"), ':', ''))  # Strip trailing ":" character
    vocab_map = vocab_map.select(*[F.col(col).alias("n3c_map_" + col) for col in vocab_map.columns])     # Add prefix to vocab map columns
    used_vocabs = observation_fact.select(F.col("parsed_vocab_code").alias("observation_fact_parsed_vocab_code")).distinct()

    # Vocab codes that are allowed to be missing from the Vocab Map file
    allowed_unmapped_vocabs = [
        "UMLS"  # These records are overloaded with COVID results, and are handled specially in the SQL logic
    ]

    vocab_report = used_vocabs.join(vocab_map, used_vocabs["observation_fact_parsed_vocab_code"] == vocab_map["n3c_map_local_prefix"], "left") \
        .withColumn(
            "included_in_vocab_map",
            F.when(F.col("n3c_map_local_prefix").isNotNull(), F.lit("T"))
            .when(F.col("observation_fact_parsed_vocab_code").isin(allowed_unmapped_vocabs), F.lit("N but okay"))
            .otherwise(F.lit("N"))
        )
    return vocab_report
