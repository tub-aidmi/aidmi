{{ config(materialized='table') }}

SELECT
    "id" AS "Id",
    COALESCE(NULLIF(TRIM("name"), ''), 'Unnamed Project') AS "Name",
    CASE
        WHEN UPPER(TRIM("project_status__c")) IN ('ACTIVE', 'AKTIV', 'ACTIVE') THEN 'Active'
        WHEN UPPER(TRIM("project_status__c")) IN ('COMPLETED', 'ABGESCHLOSSEN', 'COMPLETED') THEN 'Completed'
        WHEN UPPER(TRIM("project_status__c")) IN ('IN PLANNING', 'IN PLANUNG', 'PLANUNG', 'PLANNING') THEN 'In Planning'
        WHEN UPPER(TRIM("project_status__c")) IN ('ON HOLD', 'PAUSIERT', 'ON HOLD') THEN 'On Hold'
        WHEN UPPER(TRIM("project_status__c")) IN ('CANCELLED', 'STORNIERT', 'CANCELLED') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN NULLIF(TRIM("go_live_date__c"), '') IS NULL THEN NULL
        WHEN "go_live_date__c" = '0000-00-00' THEN NULL
        WHEN "go_live_date__c" ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE("go_live_date__c", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN "go_live_date__c" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE("go_live_date__c", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN "go_live_date__c" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE("go_live_date__c", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN "go_live_date__c" ~ '^\d{4}-\d{2}-\d{2}$' THEN "go_live_date__c"
        WHEN UPPER(TRIM("go_live_date__c")) = 'N/A' THEN NULL
        ELSE NULL
    END AS "Go_Live_Date__c",
    "account__c" AS "Account__c",
    "opportunity__c" AS "Opportunity__c",
    "id" AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}
