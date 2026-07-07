-- models/Installed_Asset__c.sql
{{ config(materialized='table') }}

SELECT
    a.asset_id AS "Id",
    a.bezeichnung AS "Name",
    a.seriennr AS "Serial_Number__c",
    -- Date parsing for Warranty_End_Date__c
    CASE
        WHEN a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(a.garantie_bis, 'DD.MM.YYYY')::TEXT
        WHEN a.garantie_bis ~ '^\d{8}$' THEN TO_DATE(a.garantie_bis, 'YYYYMMDD')::TEXT
        WHEN a.garantie_bis ~ '^\d{1,2}\/\d{1,2}\/\d{4}$' THEN TO_DATE(a.garantie_bis, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    k.kunden_nr AS "Account__c",
    p.proj_id AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS a
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    a.kd_ref = k.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
ON
    a.projekt_ref = p.proj_id
