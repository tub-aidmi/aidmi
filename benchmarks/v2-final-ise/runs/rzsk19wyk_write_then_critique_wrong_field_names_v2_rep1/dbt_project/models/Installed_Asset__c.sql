{{ config(materialized='table') }}

SELECT 
    a.asset_id AS "Id",
    COALESCE(TRIM(a.bezeichnung), 'Unknown Asset') AS "Name",
    a.seriennr AS "Serial_Number__c",
    CASE 
        WHEN TRIM(a.garantie_bis) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(a.garantie_bis), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(a.garantie_bis) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(a.garantie_bis), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    LOWER(SUBSTR(MD5('acc_' || k.kunden_nr), 1, 15)) AS "Account__c",
    p.proj_id AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(a.kd_ref) = TRIM(k.kunden_nr)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p 
    ON TRIM(a.projekt_ref) = TRIM(p.proj_id)