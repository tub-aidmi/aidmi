-- dbt model for Installed_Asset__c

{{ config(materialized='table') }}

SELECT
    MD5(assets.asset_id) AS "Id",
    COALESCE(TRIM(INITCAP(assets.bezeichnung)), 'Unknown Asset') AS "Name",
    TRIM(assets.seriennr) AS "Serial_Number__c",
    TO_CHAR(
        CASE
            WHEN assets.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN assets.garantie_bis::DATE
            WHEN assets.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(assets.garantie_bis, 'DD.MM.YYYY')
            WHEN assets.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(assets.garantie_bis, 'MM/DD/YYYY')
            WHEN assets.garantie_bis ~ '^\d{8}$' THEN TO_DATE(assets.garantie_bis, 'YYYYMMDD')
            ELSE NULL
        END, 'YYYY-MM-DD'
    ) AS "Warranty_End_Date__c",
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
