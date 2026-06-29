
{{ config(materialized='table') }}

SELECT
    TRIM(assets.asset_id) AS "Id",
    COALESCE(TRIM(assets.bezeichnung), 'Unnamed Asset') AS "Name",
    TRIM(assets.seriennr) AS "Serial_Number__c",
    CAST(TRIM(assets.garantie_bis) AS DATE) AS "Warranty_End_Date__c",
    TRIM(kunden.kunden_nr) AS "Account__c",
    TRIM(proj.proj_id) AS "Project__c",
    TRIM(assets.asset_id) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_src', 'assets') }} AS assets
LEFT JOIN
    {{ source('fixture_wrong_field_names_src', 'kunden') }} AS kunden
    ON TRIM(assets.kd_ref) = TRIM(kunden.kunden_nr)
LEFT JOIN
    {{ source('fixture_wrong_field_names_src', 'proj') }} AS proj
    ON TRIM(assets.projekt_ref) = TRIM(proj.proj_id)
