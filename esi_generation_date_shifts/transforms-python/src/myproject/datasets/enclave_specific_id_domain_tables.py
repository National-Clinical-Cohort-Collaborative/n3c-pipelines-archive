from transforms.api import transform_df, Input, Output, configure, Check
from myproject.utils import all_domains, all_id_cols, domain_primary_keys
from pyspark.sql import functions as F, Window
import logging
from transforms import expectations as E
from real_world_data_utils.hashing_utils import hash_data_partner_id_col, hash_macrovisit_id_col, hash_gpi_col, hash_domain_id_col


'''
The goal of this script is to create tenant Enclave specific identifiers for patient data and data partner sites.

The script performs the following tasks:
- Rehashes person IDs, data partner IDs, GPIs, domain primary keys and macrovisit IDs

To do (v2):
- Add a new col: Reassign new GPI to singletons (only person ID), and create consolidated EPI col
- Keep also the original person ID col as a lookup
- Add a new col to person table: matched | unmatched | no match attempted (taking into acc sites not participating in Enclave - recalculate)

'''


logger = logging.getLogger(__name__)


def transform_generator(domain, profile):
    primary_keys = domain_primary_keys.get(domain, [])

    @configure(profile=profile)
    @transform_df(
            Output("/UNITE/ESI and Date Shifts - RWD Pipeline - Dev Datastream/datasets/omop_datasets/{domain}".format(domain=domain),
            checks=[Check(E.primary_key(*primary_keys), f'Primary Key Check for {domain}', on_error='FAIL')] if primary_keys else []),
            enclave_salt_df=Input("ri.foundry.main.dataset.aede20ec-482d-47de-a731-728129c524ec"),
            df=Input("/UNITE/Phenotype Extraction - RWD Pipeline - Dev Datastream/datasets/cancer/cancer LDS/{domain}".format(domain=domain))

    )
    def compute_function(df, enclave_salt_df):

        # Get the first row and first column value
        enclave_name = enclave_salt_df.select("Enclave").first()["Enclave"]

        logger.info(f"Enclave name: {enclave_name}")

        logger.info(f"all id cols: {all_id_cols}")

        # Define a mapping of column names to their respective column hashing functions
        hash_function_map = {
            'data_partner_id': hash_data_partner_id_col,
            'macrovisit_id': hash_macrovisit_id_col,
            'global_person_id': hash_gpi_col,
        }

        # If the domain is 'manifest_harmonized', get the unique count of data partner to ensure lack of data partner collisions
        # This is important since data partner collisions would have the most widespread impact on data incorrectness in terms of rows
        if domain == 'manifest_harmonized':
            # Count unique data_partner_id values before transformation
            unique_data_partners = df.select("data_partner_id").distinct().count()

        # Iterate over the schema and apply the appropriate hash function to each column
        for domain_col in df.columns:
            if domain_col in all_id_cols:

                # Get the relevant hash function or default to hash_domain_id_col
                hash_func = hash_function_map.get(domain_col, hash_domain_id_col)
                df = df.withColumn(domain_col, hash_func(F.col(domain_col), enclave_name))

                # Ensure collisions are resolved on the primary key columns
                if primary_keys and domain_col == primary_keys[0]:

                    pk_col = primary_keys[0]  # hashed PK name

                    # Count occurrences of each hashed PK
                    df = df.withColumn("pk_count", F.count(pk_col).over(Window.partitionBy(pk_col)))

                    # For colliding PKs, assign row_number as suffix; else 0
                    collision_w = Window.partitionBy(pk_col).orderBy(F.monotonically_increasing_id())
                    df = df.withColumn(
                        "collision_suffix",
                        F.when(
                            F.col("pk_count") > 1,
                            F.row_number().over(collision_w)
                        ).otherwise(F.lit(0))
                    )

                    # Only modify PK where collision_suffix > 0
                    # Example: bit-shift and add suffix, else keep as is
                    df = df.withColumn(
                        pk_col,
                        F.when(
                            F.col("collision_suffix") > 0,
                            (F.shiftLeft(F.col(pk_col), 3)) + F.col("collision_suffix")
                        ).otherwise(F.col(pk_col))
                    )

                    # Drop helper columns
                    df = df.drop("pk_count", "collision_suffix")

        # If the domain is 'manifest_harmonized', cast 'data_partner_id' to string
        if domain == 'manifest_harmonized':
            df = df.withColumn('data_partner_id', F.col('data_partner_id').cast('string'))
            # Count unique data_partner_id values after transformation
            unique_data_partners_new = df.select("data_partner_id").distinct().count()
            if unique_data_partners != unique_data_partners_new:
                raise ValueError(
                    f"Data partner count mismatch after hashing! "
                    f"Original: {unique_data_partners}, New: {unique_data_partners_new}"
                )

        return df

    return compute_function


transforms = (transform_generator(*domain) for domain in all_domains)
