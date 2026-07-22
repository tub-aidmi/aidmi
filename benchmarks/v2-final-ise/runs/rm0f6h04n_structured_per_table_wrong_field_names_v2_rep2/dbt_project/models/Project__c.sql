{{ config(materialized='table') }}

SELECT
    -- Transform proj_id to Salesforce-style ID (a0X prefix for custom object)
    COALESCE(LOWER('a0X' || TRIM(proj.proj_id)), NULL) AS "Id",

    -- Project name from source (ensure NOT NULL with default if needed)
    COALESCE(TRIM(proj.name), 'Unknown') AS "Name",

    -- Map German status values to target enum: Active, Completed, In Planning, On Hold, Cancelled
    CASE
        WHEN LOWER(TRIM(proj.status)) = 'aktiv' THEN 'Active'
        WHEN LOWER(TRIM(proj.status)) IN ('abgeschlossen', 'erledigt') THEN 'Completed'
        WHEN LOWER(TRIM(proj.status)) LIKE '%planung%' THEN 'In Planning'
        WHEN LOWER(TRIM(proj.status)) IN ('pausiert', 'angehalten', 'auf eis gelegt') THEN 'On Hold'
        WHEN LOWER(TRIM(proj.status)) IN ('storniert', 'gesperrt', 'beendet') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",

    -- Parse go_live date — support DD.MM.YYYY, YYYYMMDD, and MM/DD/YYYY formats; NULL for missing/invalid
    CASE
        WHEN proj.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(proj.go_live, 'DD.MM.YYYY')::TEXT
        WHEN proj.go_live ~ '^\d{8}$' AND proj.go_live !~ '^0+$' THEN TO_DATE(proj.go_live, 'YYYYMMDD')::TEXT
        WHEN proj.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(proj.go_live, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",

    -- Map customer key to Salesforce Account Id (prefix 001 for standard Accounts)
    CASE
        WHEN TRIM(proj.kd) ~ '^\d+$' THEN '001' || TRIM(proj.kd)
        ELSE NULL
    END AS "Account__c",

    -- Map opportunity reference to Salesforce Opportunity Id (prefix 006 for Opportunities)
    CASE
        WHEN TRIM(proj.opp) ~ '^\d+$' THEN '006' || TRIM(proj.opp)
        ELSE NULL
    END AS "Opportunity__c",

    -- Legacy source key for row-level verification
    TRIM(proj.proj_id) AS "Legacy_Project_ID__c",

    -- Audit columns (not present in source data)
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} proj