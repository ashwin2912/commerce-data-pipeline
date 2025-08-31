#!/usr/bin/env python3
"""
Setup LocalStack for local S3 simulation
"""

import subprocess
import time
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import Settings


def check_docker():
    """Check if Docker is running"""
    try:
        subprocess.run(['docker', 'ps'], capture_output=True, check=True)
        print("✅ Docker is running")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Docker is not running or not installed")
        print("   Install Docker Desktop and make sure it's running")
        return False


def start_localstack():
    """Start LocalStack using Docker"""
    print("🚀 Starting LocalStack...")
    
    # Check if LocalStack is already running
    try:
        result = subprocess.run(
            ['docker', 'ps', '--filter', 'name=localstack', '--format', '{{.Names}}'],
            capture_output=True, text=True, check=True
        )
        if 'localstack' in result.stdout:
            print("✅ LocalStack is already running")
            return True
    except subprocess.CalledProcessError:
        pass
    
    # Start LocalStack
    try:
        subprocess.run([
            'docker', 'run', '-d',
            '--name', 'localstack',
            '--rm',
            '-p', '4566:4566',
            '-e', 'SERVICES=s3',
            '-e', 'DEBUG=1',
            'localstack/localstack:latest'
        ], check=True)
        
        print("🔄 Waiting for LocalStack to start...")
        time.sleep(10)  # Give LocalStack time to start
        
        print("✅ LocalStack started successfully")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to start LocalStack: {e}")
        return False


def test_localstack():
    """Test LocalStack S3 connection"""
    print("🧪 Testing LocalStack S3...")
    
    import boto3
    from botocore.exceptions import ClientError
    
    try:
        # Create S3 client pointing to LocalStack
        s3_client = boto3.client(
            's3',
            endpoint_url=Settings.LOCALSTACK_ENDPOINT,
            aws_access_key_id='test',
            aws_secret_access_key='test',
            region_name=Settings.AWS_REGION
        )
        
        # Create test bucket
        bucket_name = Settings.S3_BUCKET
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"✅ Created test bucket: {bucket_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyExists':
                print(f"✅ Test bucket already exists: {bucket_name}")
            else:
                raise
        
        # Test upload/download
        test_key = "test/hello.txt"
        test_content = "Hello LocalStack!"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=test_content.encode()
        )
        print(f"✅ Test upload successful: s3://{bucket_name}/{test_key}")
        
        # Test download
        response = s3_client.get_object(Bucket=bucket_name, Key=test_key)
        downloaded_content = response['Body'].read().decode()
        
        if downloaded_content == test_content:
            print("✅ Test download successful")
            
            # Cleanup
            s3_client.delete_object(Bucket=bucket_name, Key=test_key)
            print("✅ LocalStack S3 is working perfectly!")
            return True
        else:
            print("❌ Downloaded content doesn't match")
            return False
            
    except Exception as e:
        print(f"❌ LocalStack test failed: {e}")
        return False


def show_info():
    """Show LocalStack connection info"""
    print(f"""
📋 LocalStack Configuration:
   Endpoint: {Settings.LOCALSTACK_ENDPOINT}
   Bucket: {Settings.S3_BUCKET}
   Region: {Settings.AWS_REGION}
   
🔗 Access LocalStack:
   Web UI: http://localhost:4566
   S3 Console: Use AWS CLI with --endpoint-url=http://localhost:4566
   
📝 Example AWS CLI command:
   aws --endpoint-url=http://localhost:4566 s3 ls s3://{Settings.S3_BUCKET}/
""")


def main():
    print("🐳 LocalStack Setup for GA4 Pipeline")
    print("=" * 40)

    print("Available settings attributes:", dir(Settings))
    print("LOCALSTACK_ENDPOINT value:", getattr(Settings, 'LOCALSTACK_ENDPOINT', 'NOT FOUND'))
    
    # Check Docker
    if not check_docker():
        return 1
    
    # Start LocalStack
    if not start_localstack():
        return 1
    
    # Test LocalStack
    if not test_localstack():
        return 1
    
    # Show info
    show_info()
    
    print("🎉 LocalStack setup complete!")
    print("You can now run: uv run python scripts/run_pipeline.py --test")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())