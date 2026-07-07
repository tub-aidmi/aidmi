-- dbt model for Installed_Asset__c

{{ config(materialized='table') }}

SELECT
    a.asset_id AS "Id",
    COALESCE(TRIM(a.bezeichnung), a.asset_id) AS "Name",
    TRIM(a.seriennr) AS "Serial_Number__c",
    CASE
        WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(a.garantie_bis::DATE, 'YYYY-MM-DD')
        WHEN a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(a.kd_ref) AS "Account__c",
    TRIM(a.projekt_ref) AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS a
WHERE
    a.asset_id IS NOT NULL