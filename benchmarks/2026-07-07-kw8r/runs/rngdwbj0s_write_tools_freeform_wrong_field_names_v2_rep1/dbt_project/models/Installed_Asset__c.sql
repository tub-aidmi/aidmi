{{ config(materialized='table') }}

SELECT
    'ASSET_' || a.asset_id AS "Id",
    a.bezeichnung AS "Name",
    a.seriennr AS "Serial_Number__c",
    CASE
        WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN a.garantie_bis
        WHEN a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    'ACCT_' || k.kunden_nr AS "Account__c",
    CASE
        WHEN p.proj_id IS NOT NULL THEN 'PROJ_' || p.proj_id
        ELSE NULL
    END AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON a.kd_ref = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p ON a.projekt_ref = p.proj_id
