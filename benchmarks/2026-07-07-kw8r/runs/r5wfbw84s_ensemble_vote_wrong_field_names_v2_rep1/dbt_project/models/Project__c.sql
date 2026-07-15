{{ config(materialized='table') }}

SELECT
    CAST(p.proj_id AS TEXT) AS "Id",
    p.name,
    CASE
        WHEN LOWER(TRIM(p.status)) = 'aktiv' THEN 'Active'
        WHEN LOWER(TRIM(p.status)) = 'abgeschlossen' OR LOWER(TRIM(p.status)) IN ('completed', 'done') THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) = 'in planung' OR LOWER(TRIM(p.status)) IN ('in planning', 'planning', 'inplan') THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) = 'pause' OR LOWER(TRIM(p.status)) IN ('on hold', 'held', 'paused', 'stopp') THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) = 'abgebrochen' OR LOWER(TRIM(p.status)) IN ('cancelled', 'cancel', 'storniert', 'stopped') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live IS NOT NULL AND TRIM(p.go_live) != '' THEN
            -- Attempt to parse various date formats and output ISO YYYY-MM-DD
            CASE
                -- DD.MM.YYYY format (e.g. 15.03.2024)
                WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
                    TO_DATE(p.go_live, 'DD.MM.YYYY')::TEXT
                -- YYYY-MM-DD format (already ISO)
                WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN
                    p.go_live
                -- DD/MM/YYYY format
                WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN
                    TO_DATE(p.go_live, 'DD/MM/YYYY')::TEXT
                -- MM/DD/YYYY format (less likely for German data but handle it)
                WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' AND CAST(SPLIT_PART(p.go_live, '/', 1) AS INTEGER) <= 12 THEN
                    TO_DATE(p.go_live, 'MM/DD/YYYY')::TEXT
                -- YYYYMMDD format (e.g. 20240315)
                WHEN p.go_live ~ '^\d{8}$' THEN
                    TO_DATE(p.go_live, 'YYYYMMDD')::TEXT
                ELSE NULL
            END
        ELSE NULL
    END AS "Go_Live_Date__c",
    -- Account__c: map source customer number to Account Id
    -- Join kundennr from kunden table; the target Account Id format may need prefix handling
    CASE
        WHEN TRIM(k.kunden_nr) IS NOT NULL AND TRIM(k.kunden_nr) != ''
        THEN CAST('001' || k.kunden_nr AS TEXT)  -- Salesforce-style Account Id with standard 001 prefix
        ELSE NULL
    END AS "Account__c",
    -- Opportunity__c: map source opportunity reference to Opportunity Id
    -- Chance IDs likely follow similar pattern; try direct mapping or prefixed version
    CASE
        WHEN TRIM(c.chance_id) IS NOT NULL AND TRIM(c.chance_id) != ''
        THEN CAST('006' || c.chance_id AS TEXT)  -- Salesforce-style Opportunity Id with standard 006 prefix
        ELSE NULL
    END AS "Opportunity__c",
    -- Legacy project ID from source natural key
    p.proj_id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(k.kunden_nr) = REPLACE(TRIM(p.kd), 'K-', '')  -- Strip potential K- prefix from project's customer ref
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
    ON TRIM(c.chance_id) = REPLACE(TRIM(p.opp), 'O-', '')  -- Strip potential O- prefix from project's opp ref

WHERE p.proj_id IS NOT NULL