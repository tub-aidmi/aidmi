{{ config(materialized='table') }}

WITH project_data AS (
    SELECT
        p.proj_id,
        p.name,
        p.status,
        p.go_live,
        p.kd,
        p.opp,
        k.kunden_nr
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON p.kd = k.kunden_nr
),

account_ids AS (
    SELECT
        kunden_nr,
        '001' || SUBSTRING(MD5(kunden_nr), 1, 15) AS account_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
),

opportunity_ids AS (
    SELECT
        chance_id,
        '006' || SUBSTRING(MD5(chance_id), 1, 15) AS opportunity_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
)

SELECT
    'a00' || SUBSTRING(MD5(pd.proj_id), 1, 15) AS "Id",
    pd.name AS "Name",
    CASE 
        WHEN UPPER(TRIM(pd.status)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(pd.status)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(pd.status)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(pd.status)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(pd.status)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN pd.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN pd.go_live
        ELSE NULL
    END AS "Go_Live_Date__c",
    ai.account_id AS "Account__c",
    oi.opportunity_id AS "Opportunity__c",
    pd.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM project_data pd
LEFT JOIN account_ids ai ON pd.kd = ai.kunden_nr
LEFT JOIN opportunity_ids oi ON pd.opp = oi.chance_id
