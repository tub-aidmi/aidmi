-- depends_on: {{ ref('Account') }} -- This is a logical dependency, not a dbt materialization dependency
-- depends_on: {{ ref('Opportunity') }} -- This is a logical dependency, not a dbt materialization dependency

{{ config(materialized='table') }}

SELECT
    COALESCE(p.id, 'N/A') AS "Id",
    COALESCE(TRIM(p.name), 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(p.status) = 'active' THEN 'Active'
        WHEN LOWER(p.status) = 'completed' THEN 'Completed'
        WHEN LOWER(p.status) = 'in planning' THEN 'In Planning'
        WHEN LOWER(p.status) = 'on hold' THEN 'On Hold'
        WHEN LOWER(p.status) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(p.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD-MM-YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    p.client_id AS "Account__c",
    p.opportunity_ref AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p