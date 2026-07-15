{{ config(materialized='table') }}

SELECT
    LEFT(MD5('IA' || TRIM(LOWER(a.asset_id))), 15) AS "Id",
    COALESCE(TRIM(a.bezeichnung), 'Unknown Asset') AS "Name",
    TRIM(a.seriennr) AS "Serial_Number__c",
    CASE
        WHEN a.garantie_bis IS NOT NULL AND TRIM(a.garantie_bis) <> '' THEN
            CASE
                WHEN a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(a.garantie_bis), 'DD.MM.YYYY')::TEXT
                WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN LEFT(TRIM(a.garantie_bis), 10)
                ELSE NULL
            END
        ELSE NULL
    END AS "Warranty_End_Date__c",
    LEFT(MD5('AC' || TRIM(LOWER(k.kunden_nr))), 15) AS "Account__c",
    LEFT(MD5('PR' || TRIM(LOWER(p.proj_id))), 15) AS "Project__c",
    TRIM(a.asset_id) AS "Legacy_Asset_ID__c",
    CAST(CURRENT_DATE AS TEXT) AS "CreatedDate",
    CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON a.kd_ref = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
    ON a.projekt_ref = p.proj_id