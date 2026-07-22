{{ config(materialized='table') }}

WITH assets_base AS (
    SELECT 
        a.asset_id,
        a.bezeichnung,
        a.seriennr,
        a.garantie_bis,
        a.kd_ref,
        a.projekt_ref
    FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
),
assets_with_account AS (
    SELECT 
        ab.*,
        k.kunden_nr IS NOT NULL AS has_account
    FROM assets_base ab
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
        ON TRIM(ab.kd_ref) = TRIM(k.kunden_nr)
),
assets_with_project AS (
    SELECT 
        awa.*,
        p.proj_id IS NOT NULL AS has_project
    FROM assets_with_account awa
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
        ON TRIM(awa.projekt_ref) = TRIM(p.proj_id)
)

SELECT 
    -- Id: Transform asset_id (AST-xxxxx) to Salesforce-style ('ASSET-xxxxx')
    REGEXP_REPLACE(TRIM(asset_id), '^AST-(\d+)$', 'ASSET-\1') AS "Id",

    -- Name: bezeichnung with fallback for missing values  
    CASE 
        WHEN TRIM(bezeichnung) = '' OR bezeichnung IS NULL THEN 'Unnamed Asset'
        ELSE TRIM(bezeichnung)
    END AS "Name",

    -- Serial_Number__c: serial number from source
    TRIM(seriennr) AS "Serial_Number__c",

    -- Warranty_End_Date__c: parse DD.MM.YYYY or YYYY-MM-DD, output ISO format
    CASE 
        WHEN TRIM(garantie_bis) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
            TO_CHAR(TO_DATE(TRIM(garantie_bis), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(garantie_bis) ~ '^\d{4}-\d{2}-\d{2}$' THEN
            TO_CHAR(TO_DATE(TRIM(garantie_bis), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account__c: FK to Account.Id — transform kd_ref (CUST-xxxx) to CUS-xxxx format
    CASE 
        WHEN has_account THEN REGEXP_REPLACE(TRIM(kd_ref), '^CUST-(\d+)$', 'CUS-\1')
        ELSE NULL
    END AS "Account__c",

    -- Project__c: FK to Project.Id — transform projekt_ref (PROJ-xxxx) to PROJ-xxxx format
    CASE 
        WHEN has_project THEN REGEXP_REPLACE(TRIM(projekt_ref), '^PROJ-(\d+)$', 'PROJ-\1')
        ELSE NULL
    END AS "Project__c",

    -- Legacy_Asset_ID__c: direct passthrough of source natural key
    TRIM(asset_id) AS "Legacy_Asset_ID__c",

    -- CreatedDate / LastModifiedDate: placeholder timestamps (source has no audit columns)
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",

    -- IsDeleted: literal 0 (no soft-delete concept in source)
    0 AS "IsDeleted"

FROM assets_with_project