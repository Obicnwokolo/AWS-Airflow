import pandas as pd
file_postfix = datetime.now().strftime("%Y%m%d")

def upload_data_s3(
        df: pd.Dataframe,
        bucket_name: str,
        file_name: str,
        aws_access_key: str,
        aw_sectret_key: str
):
    try:
        s3_path = f"s3://{bucket_name}/{file_name}"

        storege_options = {
            "key": aws_access_key,
            "secret": aw_sectret_key,
        }

        df.to_csv(s3_path, index=False, storage=storege_options)
        print(f"Dataframe successfully uploaded to '{s3_path}.")

    except Exception as e:
        print(f"Error uploading DataFrame to s3: {e}")

if __name__ == "__main__":

    data = {}


    # AWS Credentials and s3 Details
    aws_access_key=
    aws_secret_key=
    bucket_name= 'bdus829/obinna/airflow/raw_data/'
    file_name= f'reddit_{file_postfix}'