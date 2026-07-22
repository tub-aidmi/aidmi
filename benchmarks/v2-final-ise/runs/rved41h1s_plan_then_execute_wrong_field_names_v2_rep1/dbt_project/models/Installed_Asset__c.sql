{{ config(materialized='table') }}

SELECT
    TRIM(a.asset_id) AS "Id",
    COALESCE(NULLIF(TRIM(a.bezeichnung), ''), 'Unknown Asset') AS "Name",
    TRIM(a.seriennr) AS "Serial_Number__c",
    CASE
        WHEN a.garantie_bis IS NOT NULL AND TRIM(a.garantie_bis) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(a.garantie_bis), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantie_bis IS NOT NULL AND TRIM(a.garantie_bis) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(a.garantie_bis)
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CONCAT('ACCT_', TRIM(k.kunden_nr)) AS "Account__c",
    CONCAT('PROJ_', TRIM(p.proj_id)) AS "Project__c",
    TRIM(a.asset_id) AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON TRIM(a.kd_ref) = TRIM(k.kunden_nr)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p ON TRIM(a.projekt_ref) = TRIM(p.proj_id)