{{ config(materialized='table') }}

SELECT 
    CAST(a.asset_id AS TEXT) AS "Id",
    INITCAP(TRIM(a.bezeichnung)) AS "Name",
    TRIM(a.seriennr) AS "Serial_Number__c",
    CASE 
        WHEN a.garantie_bis IS NOT NULL AND TRIM(a.garantie_bis) != '' THEN
            CASE 
                WHEN a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(a.garantie_bis), 'DD.MM.YYYY')::TEXT
                WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(a.garantie_bis), 'YYYY-MM-DD')::TEXT
                WHEN a.garantie_bis ~ '^\d{8}$' THEN TO_DATE(TRIM(a.garantie_bis), 'YYYYMMDD')::TEXT
                ELSE NULL 
            END
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    k.kunden_nr AS "Account__c",
    p.proj_id AS "Project__c",
    CAST(a.asset_id AS TEXT) AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON TRIM(a.kd_ref) = TRIM(k.kunden_nr)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p ON TRIM(a.projekt_ref) = TRIM(p.proj_id)