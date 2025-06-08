def load(table_date, load_type , Project, source_data) :

    import pandas as pd
    source_name = source_data.replace("Source_Data_", "").lower()
    parquet_path = f"/opt/airflow/dags/Data_to_Transform/{source_name}_combine_{table_date}.parquet"
    df = pd.read_parquet(parquet_path)    
    if load_type == 'incremental' :
        df.to_gbq(destination_table=f'bronze.{source_name}',project_id=Project, if_exists="append")
    else :    
        df.to_gbq(destination_table=f'bronze.{source_name}',project_id=Project, if_exists="replace")      

