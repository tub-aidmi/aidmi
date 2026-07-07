{{ config(materialized='table') }}

SELECT
    MD5(assets.asset_id) AS "Id",
    COALESCE(assets.bezeichnung, 'Unknown Asset') AS "Name", -- Name is NOT NULL
    assets.seriennr AS "Serial_Number__c",
    TO_CHAR(CASE
        WHEN assets.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(assets.garantie_bis, 'YYYY-MM-DD')
        WHEN assets.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(assets.garantie_bis, 'DD.MM.YYYY')
        WHEN assets.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(assets.garantie_bis, 'MM/DD/YYYY')
        ELSE NULL
    END, 'YYYY-MM-DD') AS "Warranty_End_Date__c",
    MD5(assets.kd_ref) AS "Account__c", -- Links to kunden.kunden_nr
    MD5(assets.projekt_ref) AS "Project__c", -- Links to proj.proj_id
    assets.asset_id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets
