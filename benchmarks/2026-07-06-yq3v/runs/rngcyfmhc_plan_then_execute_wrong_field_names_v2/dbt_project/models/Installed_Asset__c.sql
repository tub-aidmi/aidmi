{{ config(materialized='table') }}

SELECT
    MD5(assets.asset_id) AS "Id",
    COALESCE(assets.bezeichnung, 'Unknown Asset') AS "Name",
    assets.seriennr AS "Serial_Number__c",
    TO_CHAR(COALESCE(
        TO_DATE(assets.garantie_bis, 'YYYY-MM-DD'),
        TO_DATE(assets.garantie_bis, 'DD.MM.YYYY'),
        TO_DATE(assets.garantie_bis, 'MM/DD/YYYY')
    ), 'YYYY-MM-DD') AS "Warranty_End_Date__c",
    MD5(kunden.kunden_nr) AS "Account__c",
    MD5(proj.proj_id) AS "Project__c",
    assets.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON assets.kd_ref = kunden.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
    ON assets.projekt_ref = proj.proj_id
