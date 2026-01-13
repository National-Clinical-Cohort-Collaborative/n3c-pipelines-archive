
"""
# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output

from myproject.datasets import utils


@transform_df(
    Output("/UNITE/[RP-68403B] [N3C Operational] Privacy Preserving Record Linkage, PPRL Implementation Data User Request/TARGET_DATASET_PATH"),
    source_df=Input("/UNITE/[RP-68403B] [N3C Operational] Privacy Preserving Record Linkage, PPRL Implementation Data User Request/SOURCE_DATASET_PATH"),
)
def compute(source_df):
    return utils.identity(source_df)
"""