from google.cloud import storage


class GCS:
    storage_client = storage.Client()

    def list_buckets(self):

        buckets = list(self.storage_client.list_buckets())
        print(buckets)

    def save_file(self, file_path:str, gcs_file_name:str, bucket_name:str):

        bucket = self.storage_client.get_bucket(bucket_name)
        blob = bucket.blob(gcs_file_name)
        blob.upload_from_filename(file_path)

