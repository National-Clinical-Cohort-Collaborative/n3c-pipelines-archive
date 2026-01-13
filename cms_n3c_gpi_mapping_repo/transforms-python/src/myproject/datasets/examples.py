"""
# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output

from myproject.datasets import utils


@transform_df(
    Output("/UNITE/[PPRL] Centers for Medicare &amp; Medicaid Services (CMS) Release/TARGET_DATASET_PATH"),
    source_df=Input("/UNITE/[PPRL] Centers for Medicare &amp; Medicaid Services (CMS) Release/SOURCE_DATASET_PATH"),
)
def compute(source_df):
    return utils.identity(source_df)
"""
