import os
import boto3
from pathlib import Path
from datetime import datetime
from utils.logger import get_logger

logger = get_logger('s3_upload')


class s3_uploader:
    def __init__(self, bucket_name, raw_data_dir='data/raw', region='eu-west-2'):
        self.bucket_name = bucket_name
        self.raw_data_dir = raw_data_dir
        self.region = region
        self.s3 = boto3.client('s3', region_name=self.region)

    def extract_partition(self, filename):
        datepart = filename.split('_')[1]
        dt = datetime.strptime(datepart, '%Y%m%d')
        return dt.year, dt.month, dt.day

    def upload_raw_files(self, delete_after_upload=True):
        for file in Path(self.raw_data_dir).glob('articles_*.json'):
            try:
                year, month, day = self.extract_partition(file.name)
                s3_key = f"raw/news/year={year}/month={month:02}/day={day:02}/{file.name}"
                self.s3.upload_file(str(file), self.bucket_name, s3_key)
                logger.info(f"Uploaded {file} to s3://{self.bucket_name}/{s3_key}")

                if delete_after_upload:
                    file.unlink()  # deletes the local file
                    logger.info(f"Deleted local file {file}")

            except Exception as e:
                logger.error(f"Failed to upload {file.name}: {e}")


if __name__ == '__main__':
    uploader = s3_uploader(bucket_name='gnewsdata')
    uploader.upload_raw_files()

