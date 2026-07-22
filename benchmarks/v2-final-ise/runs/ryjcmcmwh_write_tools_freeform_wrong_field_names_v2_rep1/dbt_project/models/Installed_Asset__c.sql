{{ config(materialized='table') }}

SELECT
    CAST(a.asset_id AS TEXT) AS "Id",
    coalesce(trim(a.bezeichnung), 'Unknown') AS "Name",
    TRIM(a.seriennr) AS "Serial_Number__c",
    CASE
        WHEN a.garantie_bis IS NOT NULL AND a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN a.garantie_bis IS NOT NULL AND a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantie_bis IS NOT NULL AND a.garantie_bis ~ '^\d{8}$' THEN SUBSTR(a.garantie_bis, 1, 4) || '-' || SUBSTR(a.garantie_bis, 5, 2) || '-' || SUBSTR(a.garantie_bis, 7, 2)
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(a.kd_ref) AS "Account__c",
    TRIM(a.projekt_ref) AS "Project__c",
    CAST(a.asset_id AS TEXT) AS "Legacy_Asset_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
