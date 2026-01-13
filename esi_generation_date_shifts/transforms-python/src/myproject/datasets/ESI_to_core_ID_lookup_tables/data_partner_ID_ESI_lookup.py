from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F
from real_world_data_utils.hashing_utils import hash_data_partner_id_col


'''
The goal of this script is to create a lookup for enclave specific site ID (ESI)
to core N3Clinical data partner site IDs
'''


@transform_df(
        Output("ri.foundry.main.dataset.719a51f6-86a8-430d-a1a7-61fd5eb73d7f"),
        enclave_salt_df=Input("ri.foundry.main.dataset.aede20ec-482d-47de-a731-728129c524ec"),
        df=Input("ri.foundry.main.dataset.a099385e-35cb-4d0f-9dde-6c2e011ba2c0")
)
def compute_function(df, enclave_salt_df):

    # Get the first row and first column value
    enclave_name = enclave_salt_df.select("Enclave").first()["Enclave"]

    # Cast 'data_partner_id' to string
    df = df.withColumn('data_partner_id', F.col('data_partner_id').cast('string'))

    # Count unique data_partner_id values before transformation
    unique_data_partners = df.select("data_partner_id").distinct().count()

    # Cast 'enclave_data_partner_id' to string
    df = df.withColumn('enclave_data_partner_id',
        hash_data_partner_id_col(F.col('data_partner_id'), enclave_name).cast('string'))

    # Count unique data_partner_id values after transformation
    unique_data_partners_enclave = df.select("enclave_data_partner_id").distinct().count()
    if unique_data_partners != unique_data_partners_enclave:
        raise ValueError(
            f"Data partner count mismatch after hashing! "
            f"Original: {unique_data_partners}, Enclave: {unique_data_partners_enclave}"
            )

    df = df.select("data_partner_id", "enclave_data_partner_id")

    return df
