{{ config(materialized='table') }}

SELECT
    asset_id AS "Id",
    bezeichnung AS "Name",
    seriennr AS "Serial_Number__c",
    CASE 
        WHEN garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN garantie_bis
        WHEN garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    kunden.kunden_nr AS "Account__c",
    proj.proj_id AS "Project__c",
    asset_id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden 
    ON assets.kd_ref = kunden.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj 
    ON assets.projekt_ref = proj.proj_id