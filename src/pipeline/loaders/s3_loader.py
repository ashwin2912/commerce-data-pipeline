# src/ga4_pipeline/loaders/s3_loader.py
"""
S3 loader for bronze layer data.
Saves GA4 events data to S3 in parquet format with date partitioning.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
import json
import pandas as pd
import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


class S3Loader:
    """Load data to S3 bronze layer with date partitioning"""
    
    def __init__(
        self, 
        bucket_name: str,
        prefix: str = "bronze/ga4",
        region: str = "us-east-1",
        aws_profile: Optional[str] = None,
        use_localstack: bool = False,
        localstack_endpoint: str = "http://localhost:4566"
    ):
        """
        Initialize S3 loader
        
        Args:
            bucket_name: S3 bucket name
            prefix: S3 key prefix (e.g., 'bronze/ga4')
            region: AWS region
            aws_profile: AWS profile name (optional)
        """
        print(f"DEBUG S3Loader: use_localstack = {use_localstack}")
        print(f"DEBUG S3Loader: localstack_endpoint = {localstack_endpoint}")
        self.bucket_name = bucket_name
        self.prefix = prefix.rstrip('/')  # Remove trailing slash
        self.region = region
        self.use_localstack = use_localstack

        # Initialize S3 client
        if use_localstack:
            self.s3_client = boto3.client(
                's3', 
                region_name=region, 
                endpoint_url=localstack_endpoint,
                aws_access_key_id='test',
                aws_secret_access_key='test'
            )
            logger.info(f"Using LocalStack S3 at {localstack_endpoint}")
        else:
            session = boto3.Session(profile_name=aws_profile) if aws_profile else boto3.Session()
            self.s3_client = session.client('s3', region_name=region)
            logger.info(f"Using AWS S3 in region {region} with bucket {bucket_name}")
    
    def upload_events(
        self, 
        df: pd.DataFrame, 
        date: str,
        data_type: str = "events"
    ) -> str:
        """
        Upload events DataFrame to S3 with date partitioning
        
        Args:
            df: DataFrame containing events data
            date: Date string in YYYY-MM-DD format
            data_type: Type of data (e.g., 'events', 'transactions')
            
        Returns:
            S3 key where data was uploaded
        """
        if df.empty:
            logger.warning(f"Empty DataFrame provided for {date}")
            return None
        
        # Create partitioned S3 key
        year, month, day = date.split('-')
        s3_key = f"{self.prefix}/{data_type}/year={year}/month={month}/day={day}/data.parquet"
        
        try:
            # Convert DataFrame to parquet bytes
            parquet_buffer = df.to_parquet(index=False, engine='pyarrow')
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=parquet_buffer,
                ContentType='application/octet-stream'
            )
            
            logger.info(f"Uploaded {len(df)} records to s3://{self.bucket_name}/{s3_key}")
            
            # Upload metadata
            self._upload_metadata(df, date, data_type, s3_key, len(parquet_buffer))
            
            return s3_key
            
        except Exception as e:
            logger.error(f"Failed to upload to S3: {str(e)}")
            raise
    
    def _upload_metadata(
        self, 
        df: pd.DataFrame, 
        date: str, 
        data_type: str, 
        s3_key: str,
        file_size_bytes: int
    ):
        """Upload metadata file alongside the data"""
        
        metadata = {
            'date': date,
            'data_type': data_type,
            'record_count': len(df),
            'columns': list(df.columns),
            'file_size_mb': round(file_size_bytes / (1024 * 1024), 2),
            'upload_timestamp': datetime.now().isoformat(),
            's3_key': s3_key,
            'dtypes': df.dtypes.astype(str).to_dict()
        }
        
        # Create metadata S3 key (same path, different filename)
        metadata_key = s3_key.replace('/data.parquet', '/metadata.json')
        
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2),
                ContentType='application/json'
            )
            logger.info(f"Uploaded metadata to s3://{self.bucket_name}/{metadata_key}")
            
        except Exception as e:
            logger.warning(f"Failed to upload metadata: {str(e)}")
    
    def check_exists(self, date: str, data_type: str = "events") -> bool:
        """
        Check if data already exists for a given date
        
        Args:
            date: Date string in YYYY-MM-DD format
            data_type: Type of data to check
            
        Returns:
            True if data exists, False otherwise
        """
        year, month, day = date.split('-')
        s3_key = f"{self.prefix}/{data_type}/year={year}/month={month}/day={day}/data.parquet"
        
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Error checking S3 object: {str(e)}")
                raise
    
    def list_available_dates(self, data_type: str = "events", limit: int = 100) -> list:
        """
        List available dates in S3 bronze layer
        
        Args:
            data_type: Type of data to list
            limit: Maximum number of dates to return
            
        Returns:
            List of dates in YYYY-MM-DD format
        """
        prefix = f"{self.prefix}/{data_type}/"
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                Delimiter='/'
            )
            
            dates = []
            if 'CommonPrefixes' in response:
                for obj in response['CommonPrefixes']:
                    # Extract date from S3 path structure
                    # Expected: bronze/ga4/events/year=2024/month=01/day=15/
                    path_parts = obj['Prefix'].rstrip('/').split('/')
                    try:
                        year_part = [p for p in path_parts if p.startswith('year=')][0]
                        month_part = [p for p in path_parts if p.startswith('month=')][0]  
                        day_part = [p for p in path_parts if p.startswith('day=')][0]
                        
                        year = year_part.split('=')[1]
                        month = month_part.split('=')[1]
                        day = day_part.split('=')[1]
                        
                        date_str = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                        dates.append(date_str)
                        
                    except (IndexError, ValueError):
                        continue
            
            return sorted(dates, reverse=True)[:limit]
            
        except Exception as e:
            logger.error(f"Error listing S3 objects: {str(e)}")
            return []
    
    def test_connection(self) -> bool:
        """Test S3 connection and bucket access"""
        try:
            # For LocalStack, create bucket if it doesn't exist
            if self.use_localstack:
                try:
                    self.s3_client.head_bucket(Bucket=self.bucket_name)
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        logger.info(f"Creating LocalStack bucket: {self.bucket_name}")
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        raise
            else:
                # For real AWS, just check if bucket exists
                self.s3_client.head_bucket(Bucket=self.bucket_name)
            
            logger.info(f"Successfully connected to S3 bucket: {self.bucket_name}")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"S3 bucket not found: {self.bucket_name}")
            elif error_code == '403':
                logger.error(f"Access denied to S3 bucket: {self.bucket_name}")
            else:
                logger.error(f"S3 connection error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"S3 connection test failed: {str(e)}")
            return False
    
# src/ga4_pipeline/loaders/__init__.py
"""Loaders module for GA4 pipeline"""
from .s3_loader import S3Loader

__all__ = ["S3Loader"]