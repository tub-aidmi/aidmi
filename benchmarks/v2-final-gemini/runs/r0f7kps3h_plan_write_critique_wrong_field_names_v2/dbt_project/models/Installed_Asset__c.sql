{{ config(materialized='table') }}

SELECT
    ENCODE(SHA256(assets.asset_id::bytea), 'hex') AS "Id",
    COALESCE(TRIM(assets.bezeichnung), 'Unknown Asset') AS "Name",
    TRIM(assets.seriennr) AS "Serial_Number__c",
    TO_CHAR(
        CASE
            WHEN assets.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(assets.garantie_bis, 'DD.MM.YYYY')
            WHEN assets.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(assets.garantie_bis, 'YYYY-MM-DD')
            WHEN assets.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(assets.garantie_bis, 'MM/DD/YYYY')
            ELSE NULL
        END,
        'YYYY-MM-DD'
    ) AS "Warranty_End_Date__c",
    MD5(kunden.kunden_nr) AS "Account__c",
    ENCODE(SHA256(proj.proj_id::bytea), 'hex') AS "Project__c",
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
