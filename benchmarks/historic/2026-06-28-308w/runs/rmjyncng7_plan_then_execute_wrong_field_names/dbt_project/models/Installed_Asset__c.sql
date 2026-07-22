
{{ config(materialized='table') }}

SELECT
    TRIM(a.asset_id) AS "Id",
    COALESCE(TRIM(a.bezeichnung), 'Untitled Asset') AS "Name",
    TRIM(a.seriennr) AS "Serial_Number__c",
    CASE
        WHEN TRIM(a.garantie_bis) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TRIM(a.garantie_bis)::DATE, 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(a.kd_ref) AS "Account__c",
    TRIM(a.projekt_ref) AS "Project__c",
    TRIM(a.asset_id) AS "Legacy_Asset_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_src', 'assets') }} AS a
LEFT JOIN
    {{ source('fixture_wrong_field_names_src', 'kunden') }} AS k
    ON TRIM(a.kd_ref) = TRIM(k.kunden_nr)
LEFT JOIN
    {{ source('fixture_wrong_field_names_src', 'proj') }} AS p
    ON TRIM(a.projekt_ref) = TRIM(p.proj_id)
