{{ config(materialized='table') }}

SELECT
    TRIM(asset_id) AS "Id",
    INITCAP(TRIM(bezeichnung)) AS "Name",
    TRIM(seriennr) AS "Serial_Number__c",

    CASE
        WHEN TRIM(garantie_bis) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(garantie_bis), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(garantie_bis) ~ '^\d{8}$'  THEN TO_DATE(TRIM(garantie_bis), 'YYYYMMDD')::TEXT
        WHEN TRIM(garantie_bis) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(garantie_bis)
        ELSE NULL
    END AS "Warranty_End_Date__c",

    '001' || COALESCE(TRIM(k.kunden_nr), TRIM(a.kd_ref)) AS "Account__c",
    'a00' || COALESCE(TRIM(p.proj_id), TRIM(a.projekt_ref)) AS "Project__c",

    TRIM(asset_id) AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(a.kd_ref) = TRIM(k.kunden_nr)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
    ON TRIM(a.projekt_ref) = TRIM(p.proj_id)