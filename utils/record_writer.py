
def write_data_to_csv(df, path: str):
    df.write \
      .option("header", "true") \
      .mode("overwrite") \
      .csv(path)

    print("Path", path)
