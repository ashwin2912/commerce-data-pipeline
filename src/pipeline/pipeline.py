# src/ga4_pipeline/pipeline.py
"""
Main pipeline orchestrator for GA4 to Bronze layer processing.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from .extractors import BigQueryExtractor
from .loaders import S3Loader


logger = logging.getLogger(__name__)


class Pipeline:
    """GA4 to Bronze layer data pipeline"""
    
    def __init__(
        self,

        # S3 settings  
        s3_bucket: str,
        gcp_project_id: str,
        ga4_dataset_id: str,
        s3_prefix: str = "bronze/ga4",
        aws_region: str = "us-east-1",
        aws_profile: Optional[str] = None,
        
        # BigQuery settings

        gcp_credentials_path: Optional[str] = None,
        

        # LocalStack settings
        use_localstack: bool = False,
        localstack_endpoint: str = "http://localhost:4566"
    ):
        """
        Initialize the pipeline
        
        Args:
            gcp_project_id: Google Cloud project ID
            ga4_dataset_id: GA4 dataset ID in BigQuery
            gcp_credentials_path: Path to GCP service account JSON
            s3_bucket: S3 bucket for bronze layer
            s3_prefix: S3 key prefix
            aws_region: AWS region
            aws_profile: AWS profile name
        """
        self.use_localstack = use_localstack
        self.localstack_endpoint = localstack_endpoint
        # Initialize extractor
        self.extractor = BigQueryExtractor(
            project_id=gcp_project_id,
            dataset_id=ga4_dataset_id,
            credentials_path=gcp_credentials_path
        )
        
        # Initialize loader
        self.loader = S3Loader(
            bucket_name=s3_bucket,
            prefix=s3_prefix,
            region=aws_region,
            aws_profile=aws_profile,
            use_localstack=self.use_localstack,
            localstack_endpoint=self.localstack_endpoint
        )
        
        self.gcp_project_id = gcp_project_id
        self.ga4_dataset_id = ga4_dataset_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
    
    def run_daily(
        self, 
        date: Optional[str] = None, 
        skip_existing: bool = True
    ) -> Dict[str, Any]:
        """
        Run daily pipeline for a specific date
        
        Args:
            date: Date in YYYY-MM-DD format (defaults to yesterday)
            skip_existing: Skip if data already exists in S3
            
        Returns:
            Dictionary with pipeline results
        """
        # Default to yesterday if no date provided
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        logger.info(f"Starting pipeline for date: {date}")
        
        result = {
            'date': date,
            'success': False,
            'records_extracted': 0,
            's3_key': None,
            'error': None,
            'skipped': False
        }
        
        try:
            # Check if data already exists
            if skip_existing and self.loader.check_exists(date):
                logger.info(f"Data already exists for {date}, skipping")
                result['skipped'] = True
                result['success'] = True
                return result
            
            # Extract data from BigQuery
            logger.info(f"Extracting GA4 events for {date}")
            df = self.extractor.extract_events(date)
            
            if df.empty:
                logger.warning(f"No data found for {date}")
                result['error'] = 'No data found'
                return result
            
            result['records_extracted'] = len(df)
            
            # Load to S3
            logger.info(f"Loading {len(df)} records to S3")
            s3_key = self.loader.upload_events(df, date)
            result['s3_key'] = s3_key
            
            result['success'] = True
            logger.info(f"Pipeline completed successfully for {date}")
            
        except Exception as e:
            logger.error(f"Pipeline failed for {date}: {str(e)}")
            result['error'] = str(e)
        
        return result
    
    def backfill(
        self, 
        start_date: str, 
        end_date: str, 
        skip_existing: bool = True
    ) -> Dict[str, Any]:
        """
        Backfill data for a date range
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format  
            skip_existing: Skip dates that already exist in S3
            
        Returns:
            Dictionary with backfill results
        """
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        if start > end:
            raise ValueError("start_date must be before end_date")
        
        results = {
            'start_date': start_date,
            'end_date': end_date,
            'total_days': (end - start).days + 1,
            'successful_days': [],
            'failed_days': [],
            'skipped_days': [],
            'total_records': 0
        }
        
        logger.info(f"Starting backfill from {start_date} to {end_date}")
        
        current_date = start
        while current_date <= end:
            date_str = current_date.strftime('%Y-%m-%d')
            
            try:
                day_result = self.run_daily(date_str, skip_existing=skip_existing)
                
                if day_result['success']:
                    if day_result['skipped']:
                        results['skipped_days'].append(date_str)
                    else:
                        results['successful_days'].append(date_str)
                        results['total_records'] += day_result['records_extracted']
                else:
                    results['failed_days'].append({
                        'date': date_str,
                        'error': day_result['error']
                    })
                    
            except Exception as e:
                logger.error(f"Unexpected error processing {date_str}: {str(e)}")
                results['failed_days'].append({
                    'date': date_str,
                    'error': str(e)
                })
            
            current_date += timedelta(days=1)
        
        logger.info(f"Backfill completed. Success: {len(results['successful_days'])}, "
                   f"Failed: {len(results['failed_days'])}, "
                   f"Skipped: {len(results['skipped_days'])}")
        
        return results
    
    def test_connections(self) -> Dict[str, bool]:
        """Test all pipeline connections"""
        logger.info("Testing pipeline connections...")
        
        results = {
            'bigquery': False,
            's3': False
        }
        
        # Test BigQuery
        try:
            results['bigquery'] = self.extractor.test_connection()
        except Exception as e:
            logger.error(f"BigQuery connection test failed: {str(e)}")
        
        # Test S3
        try:
            results['s3'] = self.loader.test_connection()
        except Exception as e:
            logger.error(f"S3 connection test failed: {str(e)}")
        
        all_good = all(results.values())
        logger.info(f"Connection test results: {results} - All systems: {'✅' if all_good else '❌'}")
        
        return results
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get pipeline status and available data"""
        status = {
            'connections': self.test_connections(),
            'bigquery_available_dates': [],
            's3_available_dates': [],
            'missing_dates': []
        }
        
        try:
            # Get available dates from both sources
            bq_dates = self.extractor.get_available_dates(days_back=30)
            s3_dates = self.loader.list_available_dates(limit=30)
            
            status['bigquery_available_dates'] = bq_dates
            status['s3_available_dates'] = s3_dates
            
            # Find dates in BigQuery but not in S3
            status['missing_dates'] = [date for date in bq_dates if date not in s3_dates]
            
        except Exception as e:
            logger.error(f"Error getting pipeline status: {str(e)}")
        
        return status