{{ config(materialized='table') }}

SELECT
    MD5(asset_id) AS "Id",
    COALESCE(bezeichnung, 'Untitled Asset') AS "Name", -- Name is NOT NULL
    seriennr AS "Serial_Number__c",
    CASE
        WHEN garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(garantie_bis, 'DD.MM.YYYY')
        WHEN garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(garantie_bis, 'YYYY-MM-DD')
        WHEN garantie_bis ~ '^\d{8}$' THEN TO_DATE(garantie_bis, 'YYYYMMDD')
        WHEN garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(garantie_bis, 'MM/DD/YYYY')
        ELSE NULL
    END::TEXT AS "Warranty_End_Date__c", -- Cast to text for ISO YYYY-MM-DD output
    MD5(kd_ref) AS "Account__c",
    MD5(projekt_ref) AS "Project__c",
    asset_id AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP AS "CreatedDate",
    CURRENT_TIMESTAMP AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
