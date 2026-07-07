-- depends_on: {{ ref('account') }} -- depends_on: {{ ref('opportunity') }}

{{ config(materialized='table') }}

SELECT
    MD5(p.proj_id) AS "Id",
    COALESCE(p.name, 'Unknown Project') AS "Name",
    p.status AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(p.go_live AS DATE), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(p.kd) AS "Account__c",
    MD5(p.opp) AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p