{{ config(materialized='table') }}

WITH project_data AS (
    SELECT
        p.proj_id,
        p.name,
        p.status,
        p.go_live,
        p.kd,
        p.opp
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
),

account_ids AS (
    SELECT
        kunden_nr,
        '001' || LPAD(
            REGEXP_REPLACE(kunden_nr, '[^0-9]', '', 'g'),
            15,
            '0'
        ) AS account_sf_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
),

opportunity_ids AS (
    SELECT
        chance_id,
        '006' || LPAD(
            REGEXP_REPLACE(chance_id, '[^0-9]', '', 'g'),
            15,
            '0'
        ) AS opportunity_sf_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
)

SELECT
    'a00' || LPAD(
        REGEXP_REPLACE(p.proj_id, '[^0-9]', '', 'g'),
        15,
        '0'
    ) AS "Id",
    p.name AS "Name",
    
    CASE 
        WHEN UPPER(p.status) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(p.status) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(p.status) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(p.status) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(p.status) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    
    CASE 
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' 
        THEN p.go_live
        ELSE NULL
    END AS "Go_Live_Date__c",
    
    a.account_sf_id AS "Account__c",
    o.opportunity_sf_id AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM project_data p
LEFT JOIN account_ids a ON p.kd = a.kunden_nr
LEFT JOIN opportunity_ids o ON p.opp = o.chance_id
