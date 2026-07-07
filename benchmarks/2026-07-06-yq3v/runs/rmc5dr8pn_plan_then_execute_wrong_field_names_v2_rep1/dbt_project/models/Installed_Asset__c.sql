{{ config(materialized='table') }}

SELECT
    a.asset_id AS "Id",
    COALESCE(a.bezeichnung, 'Unnamed Asset') AS "Name",
    a.seriennr AS "Serial_Number__c",
    TO_CHAR(
        CASE
            WHEN a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(a.garantie_bis, 'DD.MM.YYYY')
            WHEN a.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(a.garantie_bis, 'MM/DD/YYYY')
            WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(a.garantie_bis, 'YYYY-MM-DD')
            WHEN a.garantie_bis ~ '^\d{8}$' THEN TO_DATE(a.garantie_bis, 'YYYYMMDD')
            ELSE NULL
        END,
        'YYYY-MM-DD'
    ) AS "Warranty_End_Date__c",
    k.kunden_nr AS "Account__c",
    p.proj_id AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS a
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON a.kd_ref = k.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
    ON a.projekt_ref = p.proj_id