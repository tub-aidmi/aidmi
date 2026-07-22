{{ config(materialized='table') }}
SELECT
    a."asset_id" AS "Id",
    a."bezeichnung" AS "Name",
    a."seriennr" AS "Serial_Number__c",
    CASE
        WHEN a."garantie_bis" ~ '^\d{4}-\d{2}-\d{2}$' THEN a."garantie_bis"
        WHEN a."garantie_bis" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a."garantie_bis", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a."garantie_bis" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(a."garantie_bis", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN a."garantie_bis" ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(a."garantie_bis", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CASE
        WHEN k."kunden_nr" IS NOT NULL THEN '001' || SUBSTRING(MD5(k."kunden_nr"), 1, 15)
        ELSE NULL
    END AS "Account__c",
    p."proj_id" AS "Project__c",
    a."asset_id" AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON a."kd_ref" = k."kunden_nr"
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
    ON a."projekt_ref" = p."proj_id"