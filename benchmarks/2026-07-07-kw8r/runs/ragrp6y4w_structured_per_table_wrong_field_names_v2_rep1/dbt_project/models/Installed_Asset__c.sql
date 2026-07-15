{{ config(materialized='table') }}

WITH base AS (
    SELECT 
        a.asset_id,
        a.bezeichnung,
        a.seriennr,
        a.garantie_bis,
        k.kunden_nr,
        p.proj_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
        ON TRIM(a.kd_ref) = TRIM(k.kunden_nr)
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p 
        ON TRIM(a.projekt_ref) = TRIM(p.proj_id)
)

SELECT 
    asset_id AS "Id",
    INITCAP(TRIM(bezeichnung)) AS "Name",
    seriennr AS "Serial_Number__c",
    CASE 
        WHEN garantie_bis IS NOT NULL AND TRIM(garantie_bis) != '' THEN
            CASE
                WHEN TRIM(garantie_bis) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
                    TO_DATE(TRIM(garantie_bis), 'DD.MM.YYYY')::TEXT
                WHEN TRIM(garantie_bis) ~ '^\d{4}-\d{2}-\d{2}$' THEN 
                    TO_DATE(TRIM(garantie_bis), 'YYYY-MM-DD')::TEXT
                ELSE NULL
            END
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    CASE 
        WHEN kunden_nr IS NOT NULL AND TRIM(kunden_nr) != '' 
        THEN '001' || REGEXP_REPLACE(TRIM(kunden_nr), '[^A-Z0-9]', '', 'i')
        ELSE NULL 
    END AS "Account__c",
    proj_id AS "Project__c",
    asset_id AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM base