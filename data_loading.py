import os
from snowflake.snowpark import Session
import sys
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')

def get_snowpark_session() -> Session:
    connection_parameters = {
        "ACCOUNT": "mocxhpw-kv92980",
        "USER": "snowpark_user",
        "PASSWORD": "Test@12$4",
        "ROLE": "SYSADMIN",
        "DATABASE": "SALES_DWH",  
        "SCHEMA": "SOURCE" 
    }
    
    try:
        session = Session.builder.configs(connection_parameters).create()
        logging.info("Snowpark Session information: \n%s", session.sql("SELECT CURRENT_VERSION(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()").collect())
        return session
    except Exception as e:
        logging.error("Failed to create Snowpark session: %s", e)
        sys.exit(1)

def traverse_directory(directory, file_extension) -> list:
    local_file_path = []
    file_name = []  # List to store file paths
    partition_dir = []
    logging.info(f"Traversing directory: {directory}")

    for root, dirs, files in os.walk(directory):
        logging.debug(f"Root: {root}, Dirs: {dirs}, Files: {files}")
        for file in files:
            if file.endswith(file_extension):
                file_path = os.path.join(root, file)
                file_name.append(file)
                partition_dir.append(root.replace(directory, ""))
                local_file_path.append(file_path)

    logging.info(f"Found {len(file_name)} '{file_extension}' files.")
    return file_name, partition_dir, local_file_path

def main():
    directory_path = './dataset/'
    csv_file_name, csv_partition_dir, csv_local_file_path = traverse_directory(directory_path, '.csv')
    parquet_file_name, parquet_partition_dir, parquet_local_file_path = traverse_directory(directory_path, '.parquet')
    json_file_name, json_partition_dir, json_local_file_path = traverse_directory(directory_path, '.json')
    
    stage_location = '@SALES_DWH.SOURCE.my_internal_stages'  

    session = get_snowpark_session()

    csv_index = 0
    for file_element in csv_file_name:
        try:
            put_result = session.file.put(
                csv_local_file_path[csv_index], 
                stage_location + "/" + csv_partition_dir[csv_index], 
                auto_compress=False, overwrite=True, parallel=10
            )
            logging.info(f"{file_element} => {put_result[0].status}")
        except Exception as e:
            logging.error(f"Failed to upload {file_element}: {e}")
        csv_index += 1

    parquet_index = 0
    for file_element in parquet_file_name:
        try:
            put_result = ( 
                        get_snowpark_session().file.put( 
                            parquet_local_file_path[parquet_index], 
                            stage_location+"/"+parquet_partition_dir[parquet_index], 
                            auto_compress=False, overwrite=True, parallel=10)
                        )
            logging.info(f"{file_element} => {put_result[0].status}")
        except Exception as e:
            logging.error(f"Failed to upload {file_element}: {e}")
        parquet_index+=1
    
    json_index = 0
    for file_element in json_file_name:
        try:
            put_result = ( 
                        get_snowpark_session().file.put( 
                            json_local_file_path[json_index], 
                            stage_location+"/"+json_partition_dir[json_index], 
                            auto_compress=False, overwrite=True, parallel=10)
                        )
            logging.info(f"{file_element} => {put_result[0].status}")
        except Exception as e:
            logging.error(f"Failed to upload {file_element}: {e}")
        json_index+=1  
    

if __name__ == '__main__':
    main()
