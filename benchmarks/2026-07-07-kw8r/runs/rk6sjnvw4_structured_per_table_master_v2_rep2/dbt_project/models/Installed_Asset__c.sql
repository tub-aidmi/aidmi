{{ config(materialized='table') }}

SELECT
    -- Deterministic Salesforce-style asset Id (18-char sobject ID)
    '00Q' || LEFT(MD5(a.asset_kennung), 15) AS "Id",

    -- Asset Name with fallback to satisfy NOT NULL constraint
    COALESCE(TRIM(a.asset_name), 'Asset_' || a.asset_kennung) AS "Name",

    -- Serial number (trimmed)
    TRIM(a.serien_nummer) AS "Serial_Number__c",

    -- Warranty end date: parse multiple text formats → ISO YYYY-MM-DD, NULL on failure
    CASE
        WHEN a.garantieende IS NOT NULL AND TRIM(a.garantieende) != '' THEN
            CASE
                -- DD.MM.YYYY (European)
                WHEN TRIM(a.garantieende) ~ '^\d{2}\.\d{2}\.\d{4}$'
                    THEN TO_DATE(TRIM(a.garantieende), 'DD.MM.YYYY')::TEXT

                -- YYYYMMDD (compact numeric)
                WHEN TRIM(a.garantieende) ~ '^\d{8}$'
                    THEN SUBSTR(TRIM(a.garantieende), 1, 4) || '-'
                       || SUBSTR(TRIM(a.garantieende), 5, 2) || '-'
                       || SUBSTR(TRIM(a.garantieende), 7, 2)

                -- Fallback: try common separators after normalising
                ELSE TO_DATE(
                        REGEXP_REPLACE(
                            TRIM(a.garantieende),
                            '[-/.,]', '-', 'g'
                        ),
                        'YYYY-MM-DD'
                     )::TEXT
            END
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account reference: resolve kunden_kennung → consistent Account Id pattern
    '001' || LEFT(MD5(k.kundennummer), 15) AS "Account__c",

    -- Project reference: resolve projekt_kennung → consistent Project Id pattern
    'a00' || LEFT(MD5(p.projekt_kennung), 15) AS "Project__c",

    -- Legacy natural key for row-level verification
    a.asset_kennung AS "Legacy_Asset_ID__c",

    -- Audit timestamps (source has no explicit datetimes; use model run date)
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",

    -- Deletion flag: 0 = not deleted for all imported rows
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k
    ON TRIM(a.kunden_kennung) = TRIM(k.kundennummer)
LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} p
    ON TRIM(a.projekt_kennung) = TRIM(p.projekt_kennung)