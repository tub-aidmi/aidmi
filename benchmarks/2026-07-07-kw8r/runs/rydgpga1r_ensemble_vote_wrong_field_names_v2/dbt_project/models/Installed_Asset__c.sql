{{ config(materialized='table') }}

SELECT 
    asset_id AS "Id",
    bezeichnung AS "Name",
    seriennr AS "Serial_Number__c",
    CASE 
        WHEN garantie_bis IS NULL OR TRIM(garantie_bis) = '' THEN NULL
        WHEN TRIM(garantie_bis) ~ '^\d{4}-\d{2}-\d{2}$' 
            THEN TO_DATE(TRIM(garantie_bis), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(garantie_bis) ~ '^\d{2}\.\d{2}\.\d{4}$' 
            THEN TO_DATE(TRIM(garantie_bis), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(garantie_bis) ~ '^[0-9]{8}$' 
            THEN TO_DATE(TRIM(garantie_bis), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    '001' || LEFT(MD5(COALESCE(TRIM(k.kunden_nr), TRIM(a.kd_ref))), 15) AS "Account__c",
    projekt_ref AS "Project__c",
    asset_id AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(a.kd_ref) = TRIM(k.kunden_nr)
WHERE asset_id IS NOT NULL 
  AND TRIM(asset_id) <> ''