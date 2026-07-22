{{ config(materialized='table') }}

WITH source_assets AS (
    SELECT * FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
),
source_kunden AS (
    SELECT * FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
),
source_proj AS (
    SELECT * FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
)

SELECT 
    'a1Y' || TRIM(sa.asset_id) AS "Id",
    COALESCE(NULLIF(INITCAP(TRIM(sa.bezeichnung)), ''), 'Unnamed Asset') AS "Name",
    TRIM(sa.seriennr) AS "Serial_Number__c",
    CASE 
        WHEN TRIM(sa.garantie_bis) IS NOT NULL AND TRIM(sa.garantie_bis) != '' THEN
            CASE 
                WHEN TRIM(sa.garantie_bis) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(sa.garantie_bis), 'DD.MM.YYYY'), 'YYYY-MM-DD')
                WHEN TRIM(sa.garantie_bis) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(sa.garantie_bis), 'YYYY-MM-DD'), 'YYYY-MM-DD')
                WHEN TRIM(sa.garantie_bis) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(sa.garantie_bis), 'MM/DD/YYYY'), 'YYYY-MM-DD')
            END
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    '001' || TRIM(sk.kunden_nr) AS "Account__c",
    'a0X' || TRIM(sp.proj_id) AS "Project__c",
    TRIM(sa.asset_id) AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP()::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP()::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_assets sa
LEFT JOIN source_kunden sk ON sa.kd_ref = sk.kunden_nr
LEFT JOIN source_proj sp ON sa.projekt_ref = sp.proj_id