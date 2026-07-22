{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(INITCAP(TRIM(name)), 'Unnamed Project') AS "Name",
    CASE
        WHEN LOWER(TRIM(project_status__c)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(project_status__c)) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(TRIM(project_status__c)) IN ('in planning', 'in planung', 'planung') THEN 'In Planning'
        WHEN LOWER(TRIM(project_status__c)) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(project_status__c)) IN ('cancelled', 'storniert') THEN 'Cancelled'
    END AS "Project_Status__c",
    CASE
        WHEN go_live_date__c IS NULL
          OR TRIM(go_live_date__c) = ''
          OR UPPER(TRIM(go_live_date__c)) = 'N/A'
          OR TRIM(go_live_date__c) = '0000-00-00' THEN NULL
        WHEN go_live_date__c ~ '^\d{8}$'
            AND SUBSTRING(go_live_date__c FROM 5 FOR 2) ~ '^(0[1-9]|1[0-2])$'
            AND SUBSTRING(go_live_date__c FROM 7 FOR 2) ~ '^(0[1-9]|[12]\d|3[01])$' THEN TO_DATE(go_live_date__c, 'YYYYMMDD')::TEXT
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$'
            AND SUBSTRING(go_live_date__c FROM 6 FOR 2) ~ '^(0[1-9]|1[0-2])$'
            AND SUBSTRING(go_live_date__c FROM 9 FOR 2) ~ '^(0[1-9]|[12]\d|3[01])$' THEN go_live_date__c
        WHEN go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            AND CAST(SPLIT_PART(go_live_date__c, '/', 1) AS INT) BETWEEN 1 AND 12
            AND CAST(SPLIT_PART(go_live_date__c, '/', 2) AS INT) BETWEEN 1 AND 31 THEN TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY')::TEXT
        WHEN go_live_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$'
            AND CAST(SPLIT_PART(TRIM(go_live_date__c), '.', 1) AS INT) BETWEEN 1 AND 31
            AND CAST(SPLIT_PART(TRIM(go_live_date__c), '.', 2) AS INT) BETWEEN 1 AND 12 THEN TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY')::TEXT
    END AS "Go_Live_Date__c",
    TRIM(account__c) AS "Account__c",
    TRIM(opportunity__c) AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}