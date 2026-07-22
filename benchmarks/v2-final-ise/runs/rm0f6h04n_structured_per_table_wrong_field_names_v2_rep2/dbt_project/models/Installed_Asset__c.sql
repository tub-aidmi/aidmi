{{ config(materialized='table') }}

SELECT 
    a.asset_id AS "Id",
    INITCAP(COALESCE(TRIM(a.bezeichnung), 'Unknown Asset')) AS "Name",
    TRIM(a.seriennr) AS "Serial_Number__c",
    CASE 
        WHEN a.garantie_bis IS NULL OR TRIM(a.garantie_bis) = '' THEN NULL
        WHEN a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(a.garantie_bis), 'DD.MM.YYYY')::TEXT
        ELSE NULL  -- fallback for unparseable dates (no sentinel dates)
    END AS "Warranty_End_Date__c",
    COALESCE(c.kunden_nr, a.kd_ref) AS "Account__c",
    COALESCE(p.proj_id, a.projekt_ref) AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} c 
    ON TRIM(a.kd_ref) = TRIM(c.kunden_nr)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p 
    ON TRIM(a.projekt_ref) = TRIM(p.proj_id)