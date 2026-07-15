{{ config(materialized='table') }}
SELECT
    MD5(a.asset_id) AS "Id",
    TRIM(a.bezeichnung) AS "Name",
    TRIM(a.seriennr) AS "Serial_Number__c",
    CASE
        WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN a.garantie_bis
        WHEN a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(k.kunden_nr) AS "Account__c",
    MD5(p.proj_id) AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    '1970-01-01' AS "CreatedDate",
    '1970-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON a.kd_ref = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p ON a.projekt_ref = p.proj_id