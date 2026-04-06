def write_to_mysql(df, jdbc_url, table_name, props):
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", props["user"]) \
        .option("password", props["password"]) \
        .option("driver", props["driver"]) \
        .mode("overwrite") \
        .save()