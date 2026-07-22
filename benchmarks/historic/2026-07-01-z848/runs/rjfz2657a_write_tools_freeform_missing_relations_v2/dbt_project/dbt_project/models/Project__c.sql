{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Project') AS "Name",
    COALESCE(
        CASE
            WHEN status IN ('Active', 'Completed', 'In Planning', 'On Hold', 'Cancelled') THEN status
            ELSE 'In Planning'
        END,
        'In Planning'
    ) AS "Project_Status__c",
    CASE
        WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live::date::text
        ELSE NULL
    END AS "Go_Live_Date__c",
    client_id AS "Account__c",
    opportunity_ref AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }}
