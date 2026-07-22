{{ config(materialized='table') }}
SELECT
    '02i' || LPAD(REGEXP_REPLACE(a.asset_id, '[^0-9]', ''), 15, '0') AS "Id",
    a.bezeichnung AS "Name",
    a.seriennr AS "Serial_Number__c",
    CASE WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN a.garantie_bis ELSE NULL END AS "Warranty_End_Date__c",
    '001' || LPAD(REGEXP_REPLACE(k.kunden_nr, '[^0-9]', ''), 15, '0') AS "Account__c",
    '01t' || LPAD(REGEXP_REPLACE(p.proj_id, '[^0-9]', ''), 15, '0') AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON a.kd_ref = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p ON a.projekt_ref = p.proj_id