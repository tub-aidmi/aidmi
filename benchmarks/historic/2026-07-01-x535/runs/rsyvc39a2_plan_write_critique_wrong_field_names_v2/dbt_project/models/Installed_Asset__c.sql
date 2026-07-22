{{ config(materialized='table') }}

SELECT
    MD5(assets.asset_id) AS "Id",
    COALESCE(TRIM(assets.bezeichnung), 'Asset ' || assets.asset_id) AS "Name",
    TRIM(assets.seriennr) AS "Serial_Number__c",
    NULLIF(TO_CHAR(TO_DATE(assets.garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD'), '') AS "Warranty_End_Date__c",
    MD5(kunden.kunden_nr) AS "Account__c",
    MD5(proj.proj_id) AS "Project__c",
    assets.asset_id AS "Legacy_Asset_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON assets.kd_ref = kunden.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
    ON assets.projekt_ref = proj.proj_id
