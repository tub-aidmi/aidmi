{{ config(materialized='table') }}

SELECT
    MD5(asset_id) AS "Id",
    TRIM(bezeichnung) AS "Name",
    TRIM(seriennr) AS "Serial_Number__c",
    CASE 
        WHEN garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN garantie_bis
        WHEN garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN garantie_bis ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(kd_ref) AS "Account__c",
    MD5(projekt_ref) AS "Project__c",
    asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
