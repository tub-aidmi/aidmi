{{ config(materialized='table') }}

SELECT
    CAST(a.asset_id AS TEXT) AS "Id",
    COALESCE(INITCAP(TRIM(a.bezeichnung)), 'Unnamed Asset') AS "Name",
    CAST(a.seriennr AS TEXT) AS "Serial_Number__c",
    CASE
        WHEN TRIM(a.garantie_bis) IS NOT NULL AND TRIM(a.garantie_bis) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(a.garantie_bis), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(a.garantie_bis) IS NOT NULL AND TRIM(a.garantie_bis) ~ '^\d{8}$' THEN TO_DATE(TRIM(a.garantie_bis), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(UPPER(k.kunden_nr)) AS "Account__c",
    TRIM(UPPER(p.proj_id)) AS "Project__c",
    CAST(a.asset_id AS TEXT) AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(UPPER(a.kd_ref)) = TRIM(UPPER(k.kunden_nr))
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
    ON TRIM(UPPER(a.projekt_ref)) = TRIM(UPPER(p.proj_id))