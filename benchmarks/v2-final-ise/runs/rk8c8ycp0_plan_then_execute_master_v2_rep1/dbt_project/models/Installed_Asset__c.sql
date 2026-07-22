{{ config(materialized='table') }}

SELECT 
    INITCAP(TRIM(a.asset_kennung)) AS "Id",
    CASE 
        WHEN a.asset_name IS NULL OR TRIM(a.asset_name) = '' THEN 'Unknown Asset'
        ELSE INITCAP(TRIM(a.asset_name))
    END AS "Name",
    a.serien_nummer AS "Serial_Number__c",
    CASE 
        WHEN a.garantieende IS NOT NULL AND TRIM(a.garantieende) != '' THEN
            CASE 
                -- German format DD.MM.YYYY (e.g. 15.03.2024)
                WHEN a.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(a.garantieende), 'DD.MM.YYYY')::TEXT
                -- Compact ISO-ish YYYYMMDD (e.g. 20240315)
                WHEN a.garantieende ~ '^\d{8}$' THEN TO_DATE(TRIM(a.garantieende), 'YYYYMMDD')::TEXT
                -- US format MM/DD/YYYY (e.g. 03/15/2024)
                WHEN a.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(a.garantieende), 'MM/DD/YYYY')::TEXT
                ELSE NULL
            END
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    acct."Id" AS "Account__c",
    proj."Id" AS "Project__c",
    a.asset_kennung AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
LEFT JOIN {{ ref('Account') }} acct ON INITCAP(TRIM(a.kunden_kennung)) = acct."Id"
LEFT JOIN {{ ref('Project__c') }} proj ON INITCAP(TRIM(a.projekt_kennung)) = proj."Id"