{{ config(materialized='table') }}

SELECT
    -- Id: Transform proj_id to Salesforce-style Project ID (00H prefix)
    CASE
        WHEN TRIM(proj.proj_id) ~ '^P\d+$'
            THEN '00H' || SUBSTRING(TRIM(proj.proj_id) FROM 2)
        ELSE '00H' || SUBSTR(MD5(TRIM(proj.proj_id)), 1, 15)
    END AS "Id",

    -- Name from source project table: COALESCE to satisfy NOT NULL constraint
    COALESCE(TRIM(proj.name), 'Unknown') AS "Name",

    -- Project_Status__c: Map German status terms to target English enum values
    CASE INITCAP(TRIM(proj.status))
        WHEN 'In Bearbeitung' THEN 'Active'
        WHEN 'Abgeschlossen' THEN 'Completed'
        WHEN 'In Planung' THEN 'In Planning'
        WHEN 'Auf Eisgelegt' THEN 'On Hold'
        WHEN 'Storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",

    -- Go_Live_Date__c: Parse DD.MM.YYYY format, output ISO YYYY-MM-DD or NULL if unparseable
    CASE
        WHEN TRIM(proj.go_live) ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_DATE(TRIM(proj.go_live), 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",

    -- Account__c: Transform source customer key to Salesforce Account Id (001 prefix)
    CASE
        WHEN TRIM(kunden.kunden_nr) ~ '^K\d+$'
            THEN '001' || SUBSTRING(TRIM(kunden.kunden_nr) FROM 2)
        ELSE '001' || SUBSTR(MD5(TRIM(kunden.kunden_nr)), 1, 15)
    END AS "Account__c",

    -- Opportunity__c: Transform source opportunity key to Salesforce Opportunity Id (006 prefix)
    -- Fixed: match 'O' prefix pattern (same as Opportunity model) instead of 'C'
    CASE
        WHEN TRIM(chancen.chance_id) ~ '^O\d+$'
            THEN '006' || SUBSTRING(TRIM(chancen.chance_id) FROM 2)
        ELSE '006' || SUBSTR(MD5(TRIM(chancen.chance_id)), 1, 15)
    END AS "Opportunity__c",

    -- Legacy_Project_ID__c: Raw source proj_id for row-level traceability
    TRIM(proj.proj_id) AS "Legacy_Project_ID__c",

    -- CreatedDate / LastModifiedDate: Not present in source table, default to NULL
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",

    -- IsDeleted: Soft-delete flag, not present in source, default 0 (false)
    CAST(0 AS INTEGER) AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} proj

LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} kunden
    ON TRIM(proj.kd) = TRIM(kunden.kunden_nr)

LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} chancen
    ON TRIM(proj.opp) = TRIM(chancen.chance_id)