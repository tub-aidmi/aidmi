{{ config(materialized='table') }}

SELECT 
    TRIM(asset_id) AS "Id",
    INITCAP(TRIM(bezeichnung)) AS "Name",
    TRIM(seriennr) AS "Serial_Number__c",
    CASE 
        WHEN NULLIF(TRIM(garantie_bis), '') IS NULL THEN NULL
        WHEN garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(garantie_bis, 'YYYY-MM-DD')::TEXT
        WHEN garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(garantie_bis, 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(kd_ref) AS "Account__c",
    TRIM(projekt_ref) AS "Project__c",
    TRIM(asset_id) AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }}