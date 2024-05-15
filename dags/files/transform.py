import os
import json
from pprint import pprint
from app_logger import get_logger
from files.gcp_utils.storage import GCS
import pandas as pd

logger = get_logger(__name__)

def transform_task(bucket, gcs_raw_path, gcs_transformed_path, temp_storage):
    env = 'local'
    # env = 'gcp'

    #if env is 'local' read files from temp_storage, if env is 'gcp' use gcs_transformed_storage
    products = []
    if env == 'local':
        files = [filename for filename in os.listdir(temp_storage) if filename.startswith('products_')]
        files.sort()
        for file in files:
            file_path = f"{temp_storage}{file}"
            logger.info(f"processing {file}")
            with open(file_path, 'r') as pr_file:
                pr_json = json.loads(pr_file.read())
                # print(pr_json)
                for store, pr_list in pr_json.items():
                    if store != "":
                        for pr in pr_list:
                            pr['store'] = store
                            products.append(pr)
        logger.info(f"Total products: {len(products)}")

        df = pd.DataFrame.from_records(products)
        # print(df)
        logger.info("saving file")
        with open(f"{temp_storage}products.json","w") as pr_file:
            json.dump(products,pr_file,indent=4)
        df.to_parquet(f"{temp_storage}products.parquet")
        df.to_csv(f"{temp_storage}products.csv")
        # df.to_gbq()
        GCS().save_file(f"{temp_storage}products.json", gcs_file_name=f"{gcs_transformed_path}products.json", bucket_name=bucket)
        GCS().save_file(f"{temp_storage}products.parquet", gcs_file_name=f"{gcs_transformed_path}products.parquet", bucket_name=bucket)
        GCS().save_file(f"{temp_storage}products.csv", gcs_file_name=f"{gcs_transformed_path}products.csv", bucket_name=bucket)
        logger.info("saving file complete")



transform_task("airflow-002",None,"transformed_data/","../data/")