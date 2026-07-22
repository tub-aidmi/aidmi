{{ config(materialized='table') }}
SELECT
    '002' || SUBSTRING(a.asset_id FROM '^AST-(\d+)$') AS "Id",
    a.bezeichnung AS "Name",
    a.seriennr AS "Serial_Number__c",
    CASE WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' AND TO_DATE(a.garantie_bis, 'YYYY-MM-DD') IS NOT NULL THEN a.garantie_bis ELSE NULL END AS "Warranty_End_Date__c",
    SUBSTRING(MD5(k.kunden_nr) FROM 1 FOR 18) AS "Account__c",
    p.proj_id AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON a.kd_ref = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p ON a.projekt_ref = p.proj_id