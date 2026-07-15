{{ config(materialized='table') }}
SELECT
    a.asset_id AS "Id",
    a.bezeichnung AS "Name",
    a.seriennr AS "Serial_Number__c",
    CASE WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' AND TO_DATE(a.garantie_bis, 'YYYY-MM-DD') IS NOT NULL THEN a.garantie_bis ELSE NULL END AS "Warranty_End_Date__c",
    LPAD('001' || REGEXP_REPLACE(k.kunden_nr, '-', ''), 18, '0') AS "Account__c",
    LPAD('001' || REGEXP_REPLACE(p.proj_id, '-', ''), 18, '0') AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON a.kd_ref = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p ON a.projekt_ref = p.proj_id