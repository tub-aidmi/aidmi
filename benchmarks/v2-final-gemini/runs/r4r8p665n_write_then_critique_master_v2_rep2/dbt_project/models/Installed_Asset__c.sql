-- depends_on: {{ source('fixture_master_v2_src', 'master_assets') }}
{{ config(materialized='table') }}

SELECT
    TRIM(ma.asset_kennung) AS "Id",
    COALESCE(TRIM(ma.asset_name), TRIM(ma.asset_kennung)) AS "Name",
    TRIM(ma.serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN TRIM(ma.garantieende) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TRIM(ma.garantieende)::DATE, 'YYYY-MM-DD')
        WHEN TRIM(ma.garantieende) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(ma.garantieende), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(ma.garantieende) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(ma.garantieende), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(ma.kunden_kennung) AS "Account__c",
    TRIM(ma.projekt_kennung) AS "Project__c",
    TRIM(ma.asset_kennung) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS ma