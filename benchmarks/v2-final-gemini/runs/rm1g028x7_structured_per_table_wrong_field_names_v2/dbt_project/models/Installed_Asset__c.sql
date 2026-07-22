-- depends_on: {{ ref('Account') }} {{ ref('Project__c') }}

{{ config(materialized='table') }}

SELECT
    asset.asset_id AS "Id",
    asset.bezeichnung AS "Name",
    asset.seriennr AS "Serial_Number__c",
    CASE
        WHEN asset.garantie_bis IS NULL THEN NULL
        WHEN asset.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(asset.garantie_bis AS DATE), 'YYYY-MM-DD')
        WHEN asset.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(asset.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN asset.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(asset.garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(asset.kd_ref) AS "Account__c",
    MD5(asset.projekt_ref) AS "Project__c",
    asset.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS asset
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON asset.kd_ref = kunden.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
    ON asset.projekt_ref = proj.proj_id