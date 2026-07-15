{{ config(materialized='table') }}

SELECT
    CAST(asset_kennung AS text) AS "Id",
    INITCAP(TRIM(COALESCE(asset_name, 'Unknown Asset'))) AS "Name",
    serien_nummer AS "Serial_Number__c",
    CASE
        -- Handle null, empty, N/A, and sentinel dates
        WHEN garantieende IS NULL OR TRIM(garantieende) = '' THEN NULL
        WHEN UPPER(TRIM(garantieende)) = 'N/A' THEN NULL
        WHEN TRIM(garantieende) = '0000-00-00' THEN NULL

        -- YYYY-MM-DD format (ISO standard)
        WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(garantieende), 'YYYY-MM-DD')::TEXT

        -- DD.MM.YYYY format (German/European)
        WHEN garantieende ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(garantieende), 'DD.MM.YYYY')::TEXT

        -- YYYYMMDD format (compact)
        WHEN garantieende ~ '^\d{8}$' THEN
            TO_DATE(TRIM(garantieende), 'YYYYMMDD')::TEXT

        -- MM/DD/YYYY or DD/MM/YYYY with slashes (variable digit width)
        WHEN garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
            CASE
                WHEN CAST(SPLIT_PART(TRIM(garantieende), '/', 1) AS INTEGER) > 12
                    -- First number > 12: must be DD/MM/YYYY (no month > 12)
                    THEN TO_DATE(TRIM(garantieende), 'DD/MM/YYYY')::TEXT
                ELSE
                    -- Otherwise treat as MM/DD/YYYY (US convention)
                    TO_DATE(TRIM(garantieende), 'MM/DD/YYYY')::TEXT
            END

        -- Fallback: unrecognised format → NULL
        ELSE NULL
    END AS "Warranty_End_Date__c",
    kunden_kennung AS "Account__c",
    projekt_kennung AS "Project__c",
    asset_kennung AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }}