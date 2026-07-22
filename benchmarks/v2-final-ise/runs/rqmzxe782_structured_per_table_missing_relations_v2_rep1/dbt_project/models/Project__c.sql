{{ config(materialized='table') }}

WITH account_lookup AS (
    SELECT
        id AS account_id
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
),

opportunity_lookup AS (
    SELECT
        id AS opportunity_id
    FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
)

SELECT
    CAST(p.id AS TEXT) AS "Id",
    COALESCE(NULLIF(TRIM(p.name), ''), 'Unknown Project') AS "Name",
    CASE LOWER(TRIM(p.status))
        WHEN 'active'            THEN 'Active'
        WHEN 'completed'         THEN 'Completed'
        WHEN 'in planning'       THEN 'In Planning'
        WHEN 'on hold'           THEN 'On Hold'
        WHEN 'cancelled'         THEN 'Cancelled'
        WHEN 'canceled'          THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$'  THEN TO_DATE(p.go_live, 'YYYY-MM-DD')::TEXT
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(p.go_live, 'DD.MM.YYYY')::TEXT
        WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$'   THEN TO_DATE(p.go_live, 'MM/DD/YYYY')::TEXT
        WHEN p.go_live ~ '^\d{8}$'                THEN TO_DATE(p.go_live, 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    acc.account_id  AS "Account__c",
    opp.opportunity_id AS "Opportunity__c",
    CAST(p.id AS TEXT) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p

LEFT JOIN account_lookup acc
    ON TRIM(p.client_id) = acc.account_id

LEFT JOIN opportunity_lookup opp
    ON TRIM(p.opportunity_ref) = opp.opportunity_id