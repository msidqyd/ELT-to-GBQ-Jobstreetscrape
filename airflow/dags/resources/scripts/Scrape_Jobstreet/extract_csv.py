def extract_csv(source_data,table_date, load_type, Project):
    
    import os
    import glob
    import pandas as pd
    from datetime import datetime
    import re
    filtered_files = []
    source_name = source_data.replace("Source_Data_", "").lower()
    folder_source = Fr"/opt/airflow/dags/{source_data}"
    #Read all csv file at the folder targeted.
    csv_files = glob.glob(os.path.join(folder_source, "*.csv"))
    #For the table in folder fulfill the table date criteria, will store to filtered_files.
    for file in csv_files :
        
        base = os.path.basename(file)
        match = re.search(r'_(\d{8})_\d{6}', base)
        if match:
                file_date_str = match.group(1)
                file_date = datetime.strptime(file_date_str, "%Y%m%d").strftime('%Y-%m-%d')
                if file_date == table_date:
                    filtered_files.append(file)
    
    if not filtered_files:
        raise ValueError(f"No csv files found within date {table_date}")
    #Combine all table with table date criteria.
    df_all = pd.concat([pd.read_csv(f) for f in filtered_files], ignore_index=True)
    df_all['Publish_Time'] = pd.to_datetime(df_all['Publish_Time'], errors = 'coerce')
    if df_all['Publish_Time'].dt.tz is not None:
        df_all['Publish_Time'] = df_all['Publish_Time'].dt.tz_convert('UTC').dt.tz_localize(None)
    # If the load type is incremental, need to collect the earliest date from existing silver.
    # Hence, the table for the current load data is filtered for or equal to earliet date.
    if load_type == "incremental":
        query_silver = f"""SELECT MAX(posted_time) as time_silver FROM silver.{source_name}"""
        silver_last_time = pd.read_gbq(query_silver, project_id=Project)
        last_time = silver_last_time['time_silver'].iloc[0]
        if pd.notna(last_time):
            last_time = last_time.tz_convert(None) if last_time.tzinfo else last_time
            df_all = df_all[df_all.Publish_Time >= last_time]

    df_all['Publish_Time'] = df_all['Publish_Time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    os.makedirs("/opt/airflow/dags/Data_to_Transform", exist_ok=True)
    parquet_path = f"/opt/airflow/dags/Data_to_Transform/{source_name}_combine_{table_date}.parquet"
    df_all.to_parquet(parquet_path)
    return f"{source_name} extracted successfully with {len(df_all)} records"
