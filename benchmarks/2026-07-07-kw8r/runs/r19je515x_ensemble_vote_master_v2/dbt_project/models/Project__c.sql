{{ config(materialized='table') }}

SELECT
    -- Id: Salesforce-style generated ID from projekt_kennung (prefix '00P' for custom objects)
    '00P' || LPAD(SUBSTRING(projekt_kennung FROM '[0-9]+$')::INTEGER, 9, '0') AS "Id",

    -- Name: project name from source
    TRIM(projektname) AS "Name",

    -- Project_Status__c: mapped to allowed enum values (Active, Completed, In Planning, On Hold, Cancelled)
    CASE LOWER(TRIM(projektstatus))
        WHEN 'active' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'planung' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'pausiert' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        WHEN 'storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",

    -- Go_Live_Date__c: normalize multiple date formats to ISO YYYY-MM-DD; NULL for sentinels/invalid
    CASE
        WHEN go_live_datum IS NULL OR TRIM(go_live_datum) = '' THEN NULL
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' AND go_live_datum != '0000-00-00' THEN go_live_datum
        WHEN go_live_datum ~ '^[0-9]{8}$' THEN TO_DATE(go_live_datum, 'YYYYMMDD')::TEXT
        WHEN go_live_datum ~ '^[0-9]+/[0-9]+/[0-9]{4}$' THEN TO_DATE(go_live_datum, 'MM/DD/YYYY')::TEXT
        WHEN go_live_datum ~ '^[0-9]+\.[0-9]+\.[0-9]{4}$' THEN TO_DATE(go_live_datum, 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",

    -- Account__c: Salesforce-style Account Id ('001...' prefix) derived from kunden_kennung numeric part
    CASE
        WHEN kunden_kennung IS NOT NULL AND TRIM(kunden_kennung) != '' THEN
            '001' || LPAD(SUBSTRING(kunden_kennung FROM '[0-9]+$')::INTEGER, 9, '0')
        ELSE NULL
    END AS "Account__c",

    -- Opportunity__c: Salesforce-style Opportunity Id ('006...' prefix) derived from opp_kennung_ref numeric part
    CASE
        WHEN opp_kennung_ref IS NOT NULL AND TRIM(opp_kennung_ref) != '' THEN
            '006' || LPAD(SUBSTRING(opp_kennung_ref FROM '[0-9]+$')::INTEGER, 9, '0')
        ELSE NULL
    END AS "Opportunity__c",

    -- Legacy_Project_ID__c: natural key from source (projekt_kennung)
    projekt_kennung AS "Legacy_Project_ID__c",

    -- CreatedDate: no source timestamp available; NULL
    NULL AS "CreatedDate",

    -- LastModifiedDate: no source timestamp available; NULL
    NULL AS "LastModifiedDate",

    -- IsDeleted: default to 0 (not deleted)
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_projekte') }}