-- config/queries/extract_events.sql
-- Extract all GA4 events data for a specific date
-- Parameters: {table_name}, {event_date}

SELECT 
    -- Event metadata
    event_date,
    event_timestamp,
    event_name,
    event_params,
    event_previous_timestamp,
    event_value_in_usd,
    event_bundle_sequence_id,
    event_server_timestamp_offset,
    
    -- User information
    user_id,
    user_pseudo_id,
    user_properties,
    user_first_touch_timestamp,
    user_ltv,
    
    -- Traffic source
    traffic_source.source as traffic_source,
    traffic_source.medium as traffic_medium,
    traffic_source.name as campaign_name,
    
    -- Device information
    device.category as device_category,
    device.mobile_brand_name,
    device.mobile_model_name,
    device.mobile_marketing_name,
    device.mobile_os_hardware_model,
    device.operating_system,
    device.operating_system_version,
    device.vendor_id,
    device.advertising_id,
    device.language,
    device.is_limited_ad_tracking,
    device.time_zone_offset_seconds,
    device.browser,
    device.browser_version,
    device.web_info,
    
    -- Geographic information
    geo.continent,
    geo.country,
    geo.region,
    geo.city,
    geo.sub_continent,
    geo.metro,
    
    -- App information (if applicable)
    app_info.id as app_id,
    app_info.version as app_version,
    app_info.install_store,
    app_info.firebase_app_id,
    app_info.install_source,
    
    -- Platform
    platform,
    
    -- Stream information
    stream_id,
    
    -- E-commerce data
    ecommerce.total_item_quantity,
    ecommerce.purchase_revenue_in_usd,
    ecommerce.purchase_revenue,
    ecommerce.refund_value_in_usd,
    ecommerce.refund_value,
    ecommerce.shipping_value_in_usd,
    ecommerce.shipping_value,
    ecommerce.tax_value_in_usd,
    ecommerce.tax_value,
    ecommerce.unique_items,
    ecommerce.transaction_id,
    
    -- Items (nested)
    items,
    
    -- Privacy info
    privacy_info.analytics_storage,
    privacy_info.ads_storage,
    privacy_info.uses_transient_token,
    
    -- Additional fields from your schema
    event_dimensions,
    collected_traffic_source,
    is_active_user,
    batch_event_index,
    batch_page_id,
    batch_ordering_id,
    session_traffic_source_last_click,
    publisher
    
FROM `{table_name}`
WHERE event_date = '{event_date}'
ORDER BY event_timestamp