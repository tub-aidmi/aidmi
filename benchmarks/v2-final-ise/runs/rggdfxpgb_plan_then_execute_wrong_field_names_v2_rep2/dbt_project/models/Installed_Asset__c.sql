{{ config(materialized='table') }}

SELECT
    CONCAT('a1x', TRIM(asset_id)) AS "Id",
    COALESCE(INITCAP(TRIM(bezeichnung)), 'Unknown Asset') AS "Name",
    UPPER(TRIM(seriennr)) AS "Serial_Number__c",
    CASE 
        WHEN garantie_bis IS NULL THEN NULL
        WHEN garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(garantie_bis, 'DD.MM.YYYY')::TEXT
        WHEN garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(garantie_bis, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CONCAT('a00', TRIM(kd_ref)) AS "Account__c",
    CONCAT('a0N', TRIM(projekt_ref)) AS "Project__c",
    asset_id AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }}