{{ config(materialized='table') }}

SELECT
    MD5(assets.asset_id)::TEXT AS "Id",
    COALESCE(assets.bezeichnung, 'Unknown Asset')::TEXT AS "Name",
    assets.seriennr::TEXT AS "Serial_Number__c",
    CASE
        WHEN assets.garantie_bis IS NULL THEN NULL
        WHEN assets.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(assets.garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END::TEXT AS "Warranty_End_Date__c",
    MD5(kunden.kunden_nr)::TEXT AS "Account__c",
    MD5(proj.proj_id)::TEXT AS "Project__c",
    assets.asset_id::TEXT AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON assets.kd_ref = kunden.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
    ON assets.projekt_ref = proj.proj_id