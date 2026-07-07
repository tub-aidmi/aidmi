-- depends_on: {{ ref('Account') }}
-- depends_on: {{ ref('Opportunity') }}

{{ config(materialized='table') }}

SELECT
    s.id AS "Id",
    COALESCE(s.name, s.id) AS "Name",
    CASE UPPER(TRIM(s.project_status__c))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'IN PLANNING' THEN 'In Planning'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN s.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(s.go_live_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN s.go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(s.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN s.go_live_date__c ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(s.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    s.account__c AS "Account__c",
    s.opportunity__c AS "Opportunity__c",
    s.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }} AS s