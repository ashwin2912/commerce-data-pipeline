# src/ga4_pipeline/extractors/bigquery_extractor.py
"""
BigQuery extractor for GA4 data.
Pulls all events data from GA4 BigQuery export tables.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


logger = logging.getLogger(__name__)


class BigQueryExtractor:
    """Extract GA4 events data from BigQuery"""
    
    def __init__(
        self, 
        project_id: str, 
        dataset_id: str,
        credentials_path: Optional[str] = None,
        location: str = "US"
    ):
        """
        Initialize BigQuery extractor
        
        Args:
            project_id: GCP project ID
            dataset_id: GA4 dataset ID (e.g., 'analytics_123456789')
            credentials_path: Path to service account JSON (optional)
            location: BigQuery location
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.location = location
        
        # Initialize BigQuery client
        if credentials_path:
            self.client = bigquery.Client.from_service_account_json(
                credentials_path, project=project_id
            )
        else:
            # Use default credentials (ADC)
            self.client = bigquery.Client(project=project_id)
    
    def extract_events(self, date: str) -> pd.DataFrame:
        """
        Extract all GA4 events for a specific date
        
        Args:
            date: Date in YYYY-MM-DD format
            
        Returns:
            DataFrame with all events data
        """
        # GA4 table naming: events_YYYYMMDD
        table_date = date.replace("-", "")
        table_name = f"{self.project_id}.{self.dataset_id}.events_{table_date}"
        
        # Check if table exists
        if not self._table_exists(table_name):
            logger.warning(f"Table {table_name} does not exist")
            return pd.DataFrame()
        
        query = self._build_events_query(table_name, date)
        
        logger.info(f"Extracting events data for {date} from {table_name}")
        
        try:
            # Execute query and return as DataFrame
            query_job = self.client.query(query, location=self.location)
            df = query_job.to_dataframe()
            
            logger.info(f"Successfully extracted {len(df)} events for {date}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract data for {date}: {str(e)}")
            raise
    
    def _table_exists(self, table_name: str) -> bool:
        """Check if BigQuery table exists"""
        try:
            self.client.get_table(table_name)
            return True
        except NotFound:
            return False
    
    def _load_query(self, query_name: str) -> str:
        """Load SQL query from file"""
        from pathlib import Path
        
        # Get query file path (assumes config/queries/ structure)
        query_file = Path(__file__).parent.parent.parent.parent / "config" / "queries" / f"{query_name}.sql"
        
        if not query_file.exists():
            raise FileNotFoundError(f"Query file not found: {query_file}")
        
        return query_file.read_text()
    
    def _build_events_query(self, table_name: str, date: str) -> str:
        """
        Build the BigQuery SQL query to extract all events
        
        Args:
            table_name: Full table name
            date: Date string for filtering
            
        Returns:
            SQL query string
        """
        # Format date for BigQuery (YYYYMMDD)
        event_date = date.replace("-", "")
        
        # Load query template from file
        query_template = self._load_query("extract_events")
        
        # Format with parameters
        query = query_template.format(
            table_name=table_name,
            event_date=event_date
        )
        
        return query
    
    def get_available_dates(self, days_back: int = 30) -> list:
        """
        Get list of available dates in the GA4 dataset
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            List of available dates in YYYY-MM-DD format
        """
        # Get table list from dataset
        tables = self.client.list_tables(f"{self.project_id}.{self.dataset_id}")
        
        # Filter for events tables and extract dates
        available_dates = []
        today = datetime.now().date()
        
        for table in tables:
            if table.table_id.startswith("events_"):
                try:
                    # Extract date from table name (events_YYYYMMDD)
                    date_str = table.table_id.split("_")[1]
                    date_obj = datetime.strptime(date_str, "%Y%m%d").date()
                    
                    # Only include recent dates
                    if (today - date_obj).days <= days_back:
                        formatted_date = date_obj.strftime("%Y-%m-%d")
                        available_dates.append(formatted_date)
                        
                except (ValueError, IndexError):
                    # Skip invalid table names
                    continue
        
        return sorted(available_dates, reverse=True)
    
    def test_connection(self) -> bool:
        """Test BigQuery connection and dataset access"""
        try:
            # Try to access the dataset
            dataset = self.client.get_dataset(f"{self.project_id}.{self.dataset_id}")
            logger.info(f"Successfully connected to dataset: {dataset.dataset_id}")
            return True
            
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False


# src/ga4_pipeline/extractors/__init__.py
"""Extractors module for GA4 pipeline"""
from .bigquery_extractor import BigQueryExtractor

__all__ = ["BigQueryExtractor"]