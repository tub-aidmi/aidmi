SELECT
    projekt_kennung AS "Id",
    INITCAP(TRIM(COALESCE(projektname, ''))) AS "Name",
    CASE LOWER(TRIM(COALESCE(projektstatus, '')))
        WHEN 'aktiv' THEN 'Active'
        WHEN 'active' THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'planung' THEN 'In Planning'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'pausiert' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        WHEN 'storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum IS NULL OR go_live_datum ~ '^0{4}-0{2}-0{2}$' THEN NULL
        WHEN LENGTH(TRIM(go_live_datum)) = 8 AND TRIM(go_live_datum) ~ '^\d{8}$'
            THEN TO_DATE(TRIM(go_live_datum), 'YYYYMMDD')::TEXT
        WHEN TRIM(go_live_datum) ~ '\.'
            THEN TO_DATE(TRIM(go_live_datum), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(go_live_datum) ~ '/' THEN
            TO_DATE(
                CAST(SPLIT_PART(TRIM(go_live_datum), '/', 3) AS INTEGER)::TEXT || '-' ||
                LPAD(CAST(SPLIT_PART(TRIM(go_live_datum), '/', 1) AS INTEGER)::TEXT, 2, '0') || '-' ||
                LPAD(CAST(SPLIT_PART(TRIM(go_live_datum), '/', 2) AS INTEGER)::TEXT, 2, '0'),
            'YYYY-MM-DD')::TEXT
        WHEN TRIM(go_live_datum) ~ '\-' AND LENGTH(TRIM(go_live_datum)) = 10
            THEN TO_DATE(TRIM(go_live_datum), 'YYYY-MM-DD')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    kunden_kennung AS "Account__c",
    opp_kennung_ref AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    '1900-01-01' AS "CreatedDate",
    '1900-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }}