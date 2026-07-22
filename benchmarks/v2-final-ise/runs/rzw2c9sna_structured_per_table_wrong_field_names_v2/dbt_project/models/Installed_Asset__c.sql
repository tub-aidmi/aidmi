{{ config(materialized='table') }}

SELECT 
    a.asset_id AS "Id",
    INITCAP(a.bezeichnung) AS "Name",
    a.seriennr AS "Serial_Number__c",
    CASE 
        WHEN a.garantie_bis IS NULL OR TRIM(a.garantie_bis) = '' THEN NULL
        WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(a.garantie_bis, 'YYYY-MM-DD')::TEXT
        WHEN a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(a.garantie_bis, 'DD.MM.YYYY')::TEXT
        WHEN a.garantie_bis ~ '^\d{8}$' THEN 
            SUBSTR(a.garantie_bis, 1, 4) || '-' || 
            SUBSTR(a.garantie_bis, 5, 2) || '-' || 
            SUBSTR(a.garantie_bis, 7, 2)
        ELSE NULL
    END AS "Warranty_End_Date__c",
    k.kunden_nr AS "Account__c",
    p.proj_id AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON a.kd_ref = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p ON a.projekt_ref = p.proj_id