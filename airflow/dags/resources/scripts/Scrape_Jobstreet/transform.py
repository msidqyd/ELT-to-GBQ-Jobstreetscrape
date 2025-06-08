def transformation(source_data, Project):
    from google.cloud import bigquery
    source_name = source_data.replace("Source_Data_", "").lower()
    query = f"""
    CREATE OR REPLACE TABLE silver.{source_name}  AS
    SELECT
    ROW_NUMBER() OVER (ORDER BY job_id_platform DESC) as job_id,
    job_id_platform,
    role,
    company,
    location,
    posted_time,
    url
    FROM (
        SELECT
            TRIM(CAST(Job_ID AS STRING)) AS job_id_platform,
            TRIM(CAST(Role AS STRING)) AS role,
            COALESCE(TRIM(CAST(Company AS STRING)), "Advertiser didn't show the company") AS company,
            TRIM(CAST(Location AS STRING)) AS location,
            CASE
                WHEN publish_time IS NULL THEN TIMESTAMP '1900-01-01 00:00:00'
                ELSE PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', TRIM(CAST(publish_time AS STRING)))
            END AS posted_time,
            TRIM(CAST(URL AS STRING)) AS url,
            ROW_NUMBER() OVER (PARTITION BY Job_ID ORDER BY publish_time ASC) AS dup
        FROM bronze.{source_name} 
        WHERE Job_ID IS NOT NULL AND Role IS NOT NULL AND URL IS NOT NULL AND publish_time IS NOT NULL
    )
    WHERE dup = 1
    """
    client = bigquery.Client(project=Project)
    transform_gcp = client.query(query)
    transform_gcp.result()
    print("Transformation Success !!!")
