{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'N/A') AS "Name",
    CASE UPPER(TRIM(project_status__c))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'AKTIV' THEN 'Active'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'ABGESCHLOSSEN' THEN 'Completed'
        WHEN 'IN PLANNING' THEN 'In Planning'
        WHEN 'IN PLANUNG' THEN 'In Planning'
        WHEN 'PLANUNG' THEN 'In Planning'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'PAUSIERT' THEN 'On Hold'
        WHEN 'CANCELLED' THEN 'Cancelled'
        WHEN 'STORNIERT' THEN 'Cancelled'
        ELSE 'In Planning' -- Default for NOT NULL target
    END AS "Project_Status__c",
    CASE
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_date__c::DATE
        WHEN go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live_date__c, 'DD.MM.YYYY')
        WHEN go_live_date__c ~ '^\d{8}$' THEN TO_DATE(go_live_date__c, 'YYYYMMDD')
        WHEN go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(go_live_date__c, 'MM/DD/YYYY')
        ELSE NULL
    END::TEXT AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', source_table) }}
