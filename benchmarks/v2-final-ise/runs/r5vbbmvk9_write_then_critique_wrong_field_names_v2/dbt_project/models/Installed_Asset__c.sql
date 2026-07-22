{{ config(materialized='table') }}

SELECT
    SUBSTRING(MD5(a.asset_id::text), 1 FOR 15) AS "Id",
    INITCAP(a.bezeichnung) AS "Name",
    a.seriennr AS "Serial_Number__c",
    CASE
        WHEN a.garantie_bis IS NOT NULL AND a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(a.garantie_bis, 'YYYY-MM-DD')::text
        WHEN a.garantie_bis IS NOT NULL AND a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(a.garantie_bis, 'DD.MM.YYYY')::text
        WHEN a.garantie_bis IS NOT NULL AND a.garantie_bis ~ '^\d{8}$' THEN TO_DATE(a.garantie_bis, 'YYYYMMDD')::text
        ELSE NULL
    END AS "Warranty_End_Date__c",
    '001' || SUBSTRING(MD5(k.kunden_nr::text) FROM 1 FOR 14)::text AS "Account__c",
    'a00' || LPAD(CAST(SUBSTRING(a.projekt_ref FROM 6) AS INTEGER), 12, '0') AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON a.kd_ref = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
    ON a.projekt_ref = p.proj_id