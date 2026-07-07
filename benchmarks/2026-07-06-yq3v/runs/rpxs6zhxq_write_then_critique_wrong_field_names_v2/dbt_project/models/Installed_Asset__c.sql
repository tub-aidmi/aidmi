{{ config(materialized='table') }}

SELECT
    MD5(a.asset_id) AS "Id",
    COALESCE(TRIM(a.bezeichnung), a.asset_id) AS "Name",
    TRIM(a.seriennr) AS "Serial_Number__c",
    CASE
        WHEN a.garantie_bis ~ '^\d{{4}}-\d{{2}}-\d{{2}}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(a.kd_ref) AS "Account__c",
    MD5(a.projekt_ref) AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS a