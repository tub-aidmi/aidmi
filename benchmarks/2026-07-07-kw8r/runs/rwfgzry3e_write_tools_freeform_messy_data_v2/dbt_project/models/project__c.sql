{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    CASE WHEN name IS NULL THEN 'Unnamed Project' ELSE INITCAP(TRIM(name)) END AS "Name",
    CASE LOWER(TRIM(COALESCE(project_status__c, '')))
        WHEN 'aktiv' THEN 'Active'
        WHEN 'active' THEN 'Active'
        WHEN 'planung' THEN 'In Planning'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'completed' THEN 'Completed'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'cancelled' THEN 'Cancelled'
        WHEN 'storniert' THEN 'Cancelled'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'pausiert' THEN 'On Hold'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_date__c IS NULL OR TRIM(go_live_date__c) = '' OR TRIM(go_live_date__c) ILIKE 'n/a'
            OR go_live_date__c ~ '^0{4}-0{2}-0{2}$' THEN NULL
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    CAST(account__c AS TEXT) AS "Account__c",
    CAST(opportunity__c AS TEXT) AS "Opportunity__c",
    CAST(id AS TEXT) AS "Legacy_Project_ID__c",
    '1970-01-01' AS "CreatedDate",
    '1970-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}
