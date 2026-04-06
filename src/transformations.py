from pyspark.sql.functions import col, lower, trim, when, to_date

# 1. Normalize platform names
def standardize_platforms(df):
    return df.withColumn(
        "platform_std",
        when(lower(col("platform")).isin("fb", "facebook", "face book"), "facebook")
        .when(lower(col("platform")).isin("ig", "insta", "instagram"), "instagram")
        .otherwise(lower(col("platform")))
    )

# 2. Filter billable + active
def filter_billable_campaigns(df):
    return df.filter(
        (col("campaign_status") == "ACTIVE") &
        (col("billing_status") == "Billable")
    )

# 3. Deduplicate
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def deduplicate_campaign_loads(df):
    window = Window.partitionBy("campaign_id", "event_date").orderBy(col("snapshot_ts").desc())
    return df.withColumn("rn", row_number().over(window)) \
             .filter(col("rn") == 1).drop("rn")

# 4. Clean numeric values
def clean_numeric_metrics(df):
    return df.withColumn("ad_spend", when(col("ad_spend") < 0, 0).otherwise(col("ad_spend"))) \
             .fillna({"clicks": 0, "impressions": 0, "conversions": 0})

# 5. Date normalization
def normalize_dates(df):
    return df.withColumn("event_dt", to_date(col("event_date")))

from pyspark.sql.functions import col

def enrich_campaign_metrics(df):
    return df \
        .withColumn("ctr", col("clicks") / col("impressions")) \
        .withColumn("conversion_rate", col("conversions") / col("clicks")) \
        .withColumn("cost_per_conversion", col("ad_spend") / col("conversions")) \
        .withColumn("roas", col("revenue") / col("ad_spend"))
from pyspark.sql.functions import when

def flag_spend_leakage(df):
    return df \
        .withColumn("leakage_flag",
            when((col("ad_spend") > 10000) & (col("conversions") < 50), 1).otherwise(0)
        ) \
        .withColumn("abnormal_ctr_flag",
            when(col("ctr") > 0.2, 1).otherwise(0)
        ) \
        .withColumn("zero_conversion_spend_flag",
            when((col("conversions") == 0) & (col("ad_spend") > 0), 1).otherwise(0)
        ) \
        .withColumn("anomaly_count",
            col("leakage_flag") + col("abnormal_ctr_flag") + col("zero_conversion_spend_flag")
        )