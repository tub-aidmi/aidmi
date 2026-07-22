{{ config(materialized='table') }}

SELECT
    CAST(UPPER(TRIM(p.id)) AS TEXT)                          AS "Id",
    COALESCE(INITCAP(TRIM(p.name)), 'Unknown')               AS "Name",
    CASE
        WHEN UPPER(TRIM(p.status)) = 'ACTIVE'                THEN 'Active'
        WHEN UPPER(TRIM(p.status)) = 'COMPLETED'             THEN 'Completed'
        WHEN UPPER(TRIM(p.status)) = 'IN PLANNING'           THEN 'In Planning'
        WHEN UPPER(TRIM(p.status)) = 'ON HOLD'               THEN 'On Hold'
        WHEN UPPER(TRIM(p.status)) = 'CANCELLED'             THEN 'Cancelled'
        ELSE NULL
    END                                                      AS "Project_Status__c",
    CASE
        WHEN TRIM(p.go_live) IS NOT NULL AND TRIM(p.go_live) != ''
             AND p.go_live ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TO_CHAR(TO_DATE(TRIM(p.go_live), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END                                                      AS "Go_Live_Date__c",
    TRIM(p.client_id)                                        AS "Account__c",
    TRIM(p.opportunity_ref)                                  AS "Opportunity__c",
    UPPER(TRIM(p.id))                                        AS "Legacy_Project_ID__c",
    NULL                                                     AS "CreatedDate",
    NULL                                                     AS "LastModifiedDate",
    0                                                        AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p