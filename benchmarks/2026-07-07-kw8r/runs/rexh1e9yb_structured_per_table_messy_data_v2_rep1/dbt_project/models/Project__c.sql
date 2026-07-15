{{ config(materialized='table') }}

SELECT
    id AS "Id",
    name AS "Name",
    CASE LOWER(TRIM(project_status__c))
        WHEN 'aktiv' THEN 'Active'
        WHEN 'active' THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'completed' THEN 'Completed'
        WHEN 'planung' THEN 'In Planning'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'pausiert' THEN 'On Hold'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'storniert' THEN 'Cancelled'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        -- Skip null, empty, sentinel values
        WHEN go_live_date__c IS NULL OR TRIM(go_live_date__c) = '' OR UPPER(TRIM(go_live_date__c)) = 'N/A' THEN NULL
        -- YYYYMMDD format (e.g. 20270406)
        WHEN go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        -- ISO YYYY-MM-DD but reject invalid sentinel
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' AND go_live_date__c != '0000-00-00' THEN go_live_date__c
        -- US M/D/YYYY format (e.g. 1/29/2027)
        WHEN go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        -- European DD.MM.YYYY format (e.g. 25.02.2028)
        WHEN go_live_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        -- Anything else is unparseable → NULL
        ELSE NULL
    END AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}