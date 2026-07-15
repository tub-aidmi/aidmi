{{ config(materialized='table') }}

WITH source_assets AS (
    SELECT
        id,
        name,
        serial_number__c,
        warranty_end_date__c,
        account__c,
        project__c
    FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
),

-- Map account references: assume source account__c maps to Account.legacy_customer_id__c format
-- Transform to match Salesforce-style Account Id pattern
account_mapping AS (
    SELECT
        id,
        legacy_customer_id__c,
        -- Build Salesforce-style Account Id from legacy format or pass through if already in correct format
        CASE
            WHEN id LIKE '001%' THEN id
            WHEN legacy_customer_id__c IS NOT NULL THEN INITCAP(legacy_customer_id__c)
            ELSE NULL
        END AS mapped_account_id
    FROM {{ source('fixture_messy_data_v2_src', 'account') }}
),

project_mapping AS (
    SELECT
        id,
        -- Build Salesforce-style Project Id
        CASE
            WHEN id LIKE 'a0%' OR id LIKE 'a1%' THEN id
            ELSE INITCAP(COALESCE(name, CONCAT('Project_', id)))
        END AS mapped_project_id
    FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}
)

SELECT
    -- Primary key: source id as both Id and Legacy_Asset_ID__c
    CAST(sa.id AS TEXT) AS "Id",
    COALESCE(TRIM(sa.name), 'Unknown Asset') AS "Name",
    TRIM(sa.serial_number__c) AS "Serial_Number__c",
    
    -- Warranty end date: parse from various formats to ISO YYYY-MM-DD
    CASE
        WHEN sa.warranty_end_date__c IS NULL OR TRIM(sa.warranty_end_date__c) = '' THEN NULL
        WHEN sa.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(TO_DATE(sa.warranty_end_date__c, 'YYYY-MM-DD') AS TEXT)
        WHEN sa.warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN CAST(TO_DATE(sa.warranty_end_date__c, 'MM/DD/YYYY') AS TEXT)
        WHEN sa.warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN CAST(TO_DATE(sa.warranty_end_date__c, 'DD.MM.YYYY') AS TEXT)
        WHEN sa.warranty_end_date__c ~ '^\d{8}$' THEN
            CASE
                WHEN LENGTH(sa.warranty_end_date__c) = 8 
                AND SUBSTRING(sa.warranty_end_date__c FROM 5 FOR 2) BETWEEN '01' AND '12'
                AND SUBSTRING(sa.warranty_end_date__c FROM 7 FOR 2) BETWEEN '01' AND '31'
                THEN TO_CHAR(CAST(SUBSTRING(sa.warranty_end_date__c, 1, 4) || '-' || 
                                    SUBSTRING(sa.warranty_end_date__c, 5, 2) || '-' || 
                                    SUBSTRING(sa.warranty_end_date__c, 7, 2) AS DATE), 'YYYY-MM-DD')
                ELSE NULL
            END
        ELSE NULL
    END AS "Warranty_End_Date__c",
    
    -- Account reference: map via legacy_customer_id__c -> Salesforce Account Id
    am.mapped_account_id AS "Account__c",
    
    -- Project reference: map via project id to Salesforce-style Id
    pm.mapped_project_id AS "Project__c",
    
    -- Legacy key for row-level verification
    sa.id AS "Legacy_Asset_ID__c",
    
    -- Derived timestamps (not available in source, use placeholder)
    CAST(CURRENT_DATE AS TEXT) AS "CreatedDate",
    CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",
    
    -- Deletion flag
    0::INTEGER AS "IsDeleted"

FROM source_assets sa
LEFT JOIN account_mapping am
    ON COALESCE(sa.account__c, '') = COALESCE(am.legacy_customer_id__c, '') OR
       LOWER(TRIM(sa.account__c)) = LOWER(TRIM(am.id))
LEFT JOIN project_mapping pm
    ON COALESCE(sa.project__c, '') = COALESCE(pm.id, '')