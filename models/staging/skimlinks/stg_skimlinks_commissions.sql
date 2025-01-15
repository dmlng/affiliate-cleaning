-- Staging model for ods_affiliate_skimlinks_commissions
-- Purpose: Extract and flatten JSON fields from click_details, merchant_details, 
-- and transaction_details, while organizing the query with CTEs for clarity.

with base as (
    -- CTE to select raw data from the source table
    select
        commission_id,         -- Primary key for each commission
        click_details,         -- JSON column containing click details
        merchant_details,      -- JSON column containing merchant details
        transaction_details    -- JSON column containing transaction details
    from {{ source('ods', 'ods_affiliate_skimlinks_commissions') }}
),

flattened as (
    -- CTE to extract and transform the required fields from JSON columns
    select
        commission_id,  -- Primary identifier for commissions

        -- Extract and rename fields from the click_details JSON
        DATE(JSON_UNQUOTE(JSON_EXTRACT(click_details, '$.date'))) as click_date, -- Extracted date only
        JSON_UNQUOTE(JSON_EXTRACT(click_details, '$.custom_id')) as tag,         -- Rename custom_id to tag
        JSON_UNQUOTE(JSON_EXTRACT(click_details, '$.normalized_page_url')) as normalized_page_url,  -- Page URL

        -- Extract fields from the merchant_details JSON
        JSON_UNQUOTE(JSON_EXTRACT(merchant_details, '$.merchant_name')) as merchant_name,

        -- Extract and rename fields from the transaction_details JSON
        JSON_UNQUOTE(JSON_EXTRACT(transaction_details, '$.basket.commission_type')) as commission_type,  -- Type of commission
        JSON_UNQUOTE(JSON_EXTRACT(transaction_details, '$.basket.items')) as item_count, -- Number of items ordered
        JSON_EXTRACT(transaction_details, '$.basket.publisher_amount') as publisher_amount, -- Amount paid to the publisher
        JSON_UNQUOTE(JSON_EXTRACT(transaction_details, '$.payment_status')) as payment_status,           -- Status of payment
        DATE(JSON_UNQUOTE(JSON_EXTRACT(transaction_details, '$.transaction_date'))) as transaction_date  -- Extracted transaction date only
    from base
)

-- Final output: Select all columns from the flattened CTE
select * from flattened
