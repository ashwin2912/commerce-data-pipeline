#!/usr/bin/env python3
"""
Run the GA4 to Bronze layer pipeline
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config import Settings
from src.pipeline import Pipeline

print(f"DEBUG: USE_LOCALSTACK = {Settings.USE_LOCALSTACK}")
print(f"DEBUG: S3_BUCKET = {Settings.S3_BUCKET}")
print(f"DEBUG: LOCALSTACK_ENDPOINT = {Settings.LOCALSTACK_ENDPOINT}")

def setup_logging():
    """Setup basic logging"""
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


def main():
    parser = argparse.ArgumentParser(description='GA4 to Bronze Layer Pipeline')
    parser.add_argument('--date', help='Date to process (YYYY-MM-DD). Defaults to yesterday')
    parser.add_argument('--backfill-start', help='Start date for backfill (YYYY-MM-DD)')
    parser.add_argument('--backfill-end', help='End date for backfill (YYYY-MM-DD)')
    parser.add_argument('--test', action='store_true', help='Test connections only')
    parser.add_argument('--status', action='store_true', help='Show pipeline status')
    parser.add_argument('--force', action='store_true', help='Force run even if data exists')
    
    args = parser.parse_args()
    
    setup_logging()
    
    # Validate configuration
    try:
        Settings.validate()
        print("✅ Configuration validated")
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        return 1
    
    # Initialize pipeline
    pipeline = Pipeline(
        gcp_project_id=Settings.GCP_PROJECT_ID,
        ga4_dataset_id=Settings.GA4_DATASET_ID,
        gcp_credentials_path=None,
        s3_bucket=Settings.S3_BUCKET,
        s3_prefix=Settings.S3_PREFIX,
        aws_region=Settings.AWS_REGION,
        aws_profile=Settings.AWS_PROFILE,
        use_localstack=Settings.USE_LOCALSTACK,
        localstack_endpoint=Settings.LOCALSTACK_ENDPOINT
    )
    
    # Handle different commands
    if args.test:
        print("\n🧪 Testing connections...")
        results = pipeline.test_connections()
        for service, status in results.items():
            print(f"   {service}: {'✅' if status else '❌'}")
        return 0 if all(results.values()) else 1
    
    if args.status:
        print("\n📊 Pipeline Status:")
        status = pipeline.get_pipeline_status()
        
        print(f"  Connections:")
        for service, connected in status['connections'].items():
            print(f"    {service}: {'✅' if connected else '❌'}")
        
        print(f"  BigQuery dates available: {len(status['bigquery_available_dates'])}")
        print(f"  S3 dates available: {len(status['s3_available_dates'])}")
        print(f"  Missing in S3: {len(status['missing_dates'])}")
        
        if status['missing_dates']:
            print(f"  Missing dates: {status['missing_dates'][:5]}")
            if len(status['missing_dates']) > 5:
                print(f"    ... and {len(status['missing_dates']) - 5} more")
        
        return 0
    
    if args.backfill_start and args.backfill_end:
        print(f"\n🔄 Running backfill from {args.backfill_start} to {args.backfill_end}")
        
        results = pipeline.backfill(
            start_date=args.backfill_start,
            end_date=args.backfill_end,
            skip_existing=not args.force
        )
        
        print(f"\n📋 Backfill Results:")
        print(f"  Total days: {results['total_days']}")
        print(f"  Successful: {len(results['successful_days'])}")
        print(f"  Failed: {len(results['failed_days'])}")
        print(f"  Skipped: {len(results['skipped_days'])}")
        print(f"  Total records: {results['total_records']:,}")
        
        if results['failed_days']:
            print(f"\n❌ Failed days:")
            for failure in results['failed_days']:
                print(f"    {failure['date']}: {failure['error']}")
        
        return 0 if not results['failed_days'] else 1
    
    # Single day run
    target_date = args.date or (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"\n🚀 Running pipeline for {target_date}")
    
    result = pipeline.run_daily(
        date=target_date,
        skip_existing=not args.force
    )
    
    if result['success']:
        if result['skipped']:
            print(f"✅ Skipped {target_date} (data already exists)")
        else:
            print(f"✅ Successfully processed {result['records_extracted']:,} records")
            print(f"   Saved to: s3://{Settings.S3_BUCKET}/{result['s3_key']}")
        return 0
    else:
        print(f"❌ Failed: {result['error']}")
        return 1


if __name__ == "__main__":
    sys.exit(main())