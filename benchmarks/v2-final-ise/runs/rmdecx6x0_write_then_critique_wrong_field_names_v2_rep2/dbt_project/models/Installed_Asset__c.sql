{{ config(materialized='table') }}

SELECT
    a.asset_id AS "Id",
    COALESCE(INITCAP(TRIM(a.bezeichnung)), 'Unknown Asset') AS "Name",
    a.seriennr AS "Serial_Number__c",
    CASE
        WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(a.garantie_bis, 'YYYY-MM-DD')::TEXT
        WHEN a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(a.garantie_bis, 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    '001' || SUBSTRING(MD5(c.kunden_nr), 1, 15) AS "Account__c",
    a.projekt_ref AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} c
    ON a.kd_ref = c.kunden_nr