{{ config(materialized='table') }}

WITH asset_data AS (
    SELECT
        a.asset_id,
        a.bezeichnung,
        a.seriennr,
        a.garantie_bis,
        a.kd_ref,
        a.projekt_ref
    FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
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

project_ids AS (
    SELECT
        proj_id,
        'a00' || LPAD(
            REGEXP_REPLACE(proj_id, '[^0-9]', '', 'g'),
            15,
            '0'
        ) AS project_sf_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
)

SELECT
    'a01' || LPAD(
        REGEXP_REPLACE(a.asset_id, '[^0-9]', '', 'g'),
        15,
        '0'
    ) AS "Id",
    a.bezeichnung AS "Name",
    a.seriennr AS "Serial_Number__c",
    
    CASE 
        WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' 
        THEN a.garantie_bis
        ELSE NULL
    END AS "Warranty_End_Date__c",
    
    ac.account_sf_id AS "Account__c",
    p.project_sf_id AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM asset_data a
LEFT JOIN account_ids ac ON a.kd_ref = ac.kunden_nr
LEFT JOIN project_ids p ON a.projekt_ref = p.proj_id
