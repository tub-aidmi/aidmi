{{ config(materialized='table') }}

SELECT 
    'a00' || TRIM(asset_id) AS "Id",
    COALESCE(TRIM(INITCAP(bezeichnung)), 'Unknown Asset') AS "Name",
    TRIM(seriennr) AS "Serial_Number__c",
    CASE 
        WHEN TRIM(garantie_bis) IS NULL OR TRIM(garantie_bis) = '' THEN NULL
        WHEN TRIM(garantie_bis) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(garantie_bis), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(garantie_bis) ~ '^\d{8}$' THEN TO_DATE(TRIM(garantie_bis), 'YYYYMMDD')::TEXT
        WHEN TRIM(garantie_bis) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(garantie_bis), 'YYYY-MM-DD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CASE 
        WHEN TRIM(kd_ref) IS NOT NULL AND TRIM(kd_ref) != '' THEN '001' || TRIM(kd_ref)
        ELSE NULL
    END AS "Account__c",
    CASE 
        WHEN TRIM(projekt_ref) IS NOT NULL AND TRIM(projekt_ref) != '' THEN 'a00' || TRIM(projekt_ref)
        ELSE NULL
    END AS "Project__c",
    TRIM(asset_id) AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
WHERE TRIM(asset_id) IS NOT NULL AND TRIM(asset_id) != ''