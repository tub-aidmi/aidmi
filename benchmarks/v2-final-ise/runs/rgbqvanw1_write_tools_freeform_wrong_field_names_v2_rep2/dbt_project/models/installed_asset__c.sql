{{ config(materialized='table') }}

SELECT
    CONCAT('701', LEFT(MD5(asset_id), 17)) AS "Id",
    INITCAP(TRIM(bezeichnung)) AS "Name",
    TRIM(seriennr) AS "Serial_Number__c",
    CASE
        WHEN garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(garantie_bis, 'DD.MM.YYYY')::TEXT
        WHEN garantie_bis ~ '^\d{8}$' THEN TO_DATE(garantie_bis, 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CONCAT('001', LEFT(MD5(kd_ref), 17)) AS "Account__c",
    CONCAT('500', LEFT(MD5(projekt_ref), 17)) AS "Project__c",
    asset_id AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
