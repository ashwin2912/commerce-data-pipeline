# E-commerce Data Pipeline

A general-purpose data pipeline for e-commerce analytics, designed to extract GA4 data and build customer funnel analysis datasets.

## Overview

This pipeline extracts raw e-commerce event data from Google Analytics 4 (via BigQuery) and processes it into structured datasets for customer funnel analysis. Built with a bronze-silver-gold data architecture pattern.

## Key Features

- Extract all GA4 events data from BigQuery
- Load data into S3 bronze layer in parquet format
- Support for batch processing and backfill operations
- Configurable for different e-commerce platforms and data sources
- Designed for customer journey and funnel analysis use cases

## Architecture

```
GA4 → BigQuery → Pipeline → S3 Bronze → Snowflake Gold
```

- **Bronze Layer**: Raw GA4 events data stored in S3
- **Silver Layer**: Cleaned and transformed data (future)
- **Gold Layer**: Analytics-ready datasets in Snowflake (future)

## Use Cases

- Customer funnel analysis (page view → add to cart → purchase)
- Customer journey mapping across touchpoints
- E-commerce conversion optimization
- Attribution modeling and marketing analysis
- Custom analytics dashboards and reporting

## Prerequisites

- Google Analytics 4 property with BigQuery export enabled
- Google Cloud Platform project with BigQuery API access
- AWS account with S3 access (for bronze layer storage)
- Python 3.9+

## Quick Start

1. **Clone and setup**:
```bash
git clone <repo>
cd commerce-data-pipeline
uv sync
cp .env.example .env
```

2. **Configure environment**:
```env
GCP_PROJECT_ID=your-project-id
GA4_DATASET_ID=analytics_123456789
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
S3_BUCKET=your-bronze-bucket
```

3. **Test extraction**:
```bash
uv run python scripts/test_extractor.py
```

4. **Run pipeline**:
```bash
uv run python scripts/run_daily.py
```

## Data Output

The pipeline outputs comprehensive e-commerce event data including:
- Customer interactions (page views, clicks, form submissions)
- E-commerce events (product views, cart additions, purchases)
- User and session information
- Traffic source attribution
- Device and geographic data

Perfect for building customer funnel analysis, cohort studies, and conversion optimization insights.

## Configuration

All configuration is handled through environment variables. See `.env.example` for required settings.

## Development

- `src/ga4_pipeline/` - Main pipeline package
- `config/` - Configuration and SQL queries  
- `scripts/` - Executable pipeline scripts
- `tests/` - Test suite