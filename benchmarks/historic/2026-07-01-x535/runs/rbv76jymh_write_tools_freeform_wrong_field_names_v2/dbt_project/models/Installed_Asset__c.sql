{{ config(materialized='table') }}

SELECT
    MD5(assets.asset_id) AS "Id",
    COALESCE(assets.bezeichnung, 'Unknown Asset') AS "Name",
    assets.seriennr AS "Serial_Number__c",
    CASE
        WHEN assets.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(assets.garantie_bis::DATE, 'YYYY-MM-DD') -- YYYY-MM-DD
        WHEN assets.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(assets.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
        WHEN assets.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(assets.garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- MM/DD/YYYY
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(assets.kd_ref) AS "Account__c",
    MD5(assets.projekt_ref) AS "Project__c",
    assets.asset_id AS "Legacy_Asset_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets
