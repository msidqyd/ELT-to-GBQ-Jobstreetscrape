def validation_data(source_data, Project):
    import logging
    from google.cloud import bigquery

    source_name = source_data.replace("Source_Data_", "").lower()
    client = bigquery.Client(project=Project)

    query_1 = f"""
    SELECT COUNT(*) as null_count
    FROM silver.{source_name} 
    WHERE job_id_platform IS NULL
    OR role IS NULL
    OR company IS NULL
    OR posted_time IS NULL
    OR url IS NULL
    """

    query_2 = f"""
    SELECT job_id_platform
    FROM silver.{source_name} 
    GROUP BY job_id_platform
    HAVING COUNT(*) > 1
    """

    result_null = client.query(query_1).result()
    result_dup = client.query(query_2).result()

    null_count = [row["null_count"] for row in result_null][0]
    dup_list = [row["job_id_platform"] for row in result_dup]
    dup_count = len(dup_list)

    logging.info(f"{null_count} null values detected and {dup_count} duplicate job_id_platform detected.")

