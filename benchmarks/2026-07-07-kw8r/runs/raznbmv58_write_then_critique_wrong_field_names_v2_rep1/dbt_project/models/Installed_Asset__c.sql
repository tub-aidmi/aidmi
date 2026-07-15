{{ config(materialized='table') }}
SELECT
    MD5(assets.asset_id) || '000000000000' AS "Id",
    COALESCE(NULLIF(TRIM(assets.bezeichnung), ''), 'Unknown Asset') AS "Name",
    TRIM(assets.seriennr) AS "Serial_Number__c",
    CASE WHEN NULLIF(TRIM(assets.garantie_bis), '') ~ '^\d{4}-\d{2}-\d{2}$' THEN NULLIF(TRIM(assets.garantie_bis), '') ELSE NULL END AS "Warranty_End_Date__c",
    MD5(kunden.kunden_nr) || '000000000000' AS "Account__c",
    MD5(proj.proj_id) || '000000000000' AS "Project__c",
    assets.asset_id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden ON assets.kd_ref = kunden.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj ON assets.projekt_ref = proj.proj_id