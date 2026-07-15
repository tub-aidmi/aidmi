{{ config(materialized='table') }}

SELECT
    asset_id AS "Id",
    COALESCE(NULLIF(TRIM(bezeichnung), ''), 'Unknown') AS "Name",
    seriennr AS "Serial_Number__c",
    CASE
        WHEN garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN garantie_bis
        WHEN garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN garantie_bis ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    k.kunden_nr AS "Account__c",
    p.proj_id AS "Project__c",
    asset_id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON a.kd_ref = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p ON a.projekt_ref = p.proj_id
