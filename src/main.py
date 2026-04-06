from spark_session import init_spark, close_spark
from reader import read_campaign_data
from transformations import *
from aggregations import aggregate_campaign_triage
from writer import write_to_mysql
import config

spark = init_spark("Ad Spend Leakage Pipeline")

df = read_campaign_data(spark, "data/ads_campaign_daily.csv")

df = standardize_platforms(df)
df = normalize_dates(df)
df = filter_billable_campaigns(df)
df = deduplicate_campaign_loads(df)
df = clean_numeric_metrics(df)

df = enrich_campaign_metrics(df)
df = flag_spend_leakage(df)

df_final = aggregate_campaign_triage(df)

write_to_mysql(df_final, config.MYSQL_URL, config.TABLE_NAME, config.PROPS)

close_spark(spark)