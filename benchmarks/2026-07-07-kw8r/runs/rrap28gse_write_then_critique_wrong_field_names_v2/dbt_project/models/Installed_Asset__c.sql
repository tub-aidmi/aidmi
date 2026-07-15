{{ config(materialized='table') }}

SELECT
    a.asset_id AS "Id",
    COALESCE(a.bezeichnung, 'Unknown Asset') AS "Name",
    a.seriennr AS "Serial_Number__c",
    CASE
        WHEN a.garantie_bis IS NULL THEN NULL
        WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantie_bis ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    '001' || REGEXP_REPLACE(k.kunden_nr, '\D', '', 'g') AS "Account__c",
    p.proj_id AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON a.kd_ref = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p ON a.projekt_ref = p.proj_id