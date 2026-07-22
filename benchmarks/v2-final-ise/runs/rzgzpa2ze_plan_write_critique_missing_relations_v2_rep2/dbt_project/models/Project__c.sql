{{ config(materialized='table') }}

SELECT
    TRIM(UPPER(id)) AS "Id",
    COALESCE(INITCAP(TRIM(name)), 'Unnamed Project') AS "Name",
    CASE INITCAP(TRIM(status)) WHEN 'Active' THEN 'Active' WHEN 'Completed' THEN 'Completed' WHEN 'In Planning' THEN 'In Planning' WHEN 'On Hold' THEN 'On Hold' WHEN 'Cancelled' THEN 'Cancelled' ELSE NULL END AS "Project_Status__c",
    COALESCE(TO_DATE(TRIM(go_live), 'YYYY-MM-DD'), TO_DATE(TRIM(go_live), 'DD.MM.YYYY'), TO_DATE(TRIM(go_live), 'YYYYMMDD'), TO_DATE(TRIM(go_live), 'MM/DD/YYYY'), NULL)::TEXT AS "Go_Live_Date__c",
    TRIM(UPPER(client_id)) AS "Account__c",
    TRIM(UPPER(opportunity_ref)) AS "Opportunity__c",
    TRIM(id) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }}