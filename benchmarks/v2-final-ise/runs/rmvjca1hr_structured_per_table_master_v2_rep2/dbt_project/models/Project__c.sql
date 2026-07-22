SELECT
    p.projekt_kennung AS "Id",
    INITCAP(TRIM(COALESCE(p.projektname, ''))) AS "Name",
    CASE LOWER(TRIM(COALESCE(p.projektstatus, '')))
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
        WHEN p.go_live_datum IS NULL OR TRIM(p.go_live_datum) = '' THEN NULL
        WHEN p.go_live_datum ~ '^0{4}-0{2}-0{2}$' THEN NULL
        WHEN LENGTH(TRIM(p.go_live_datum)) = 8 AND TRIM(p.go_live_datum) ~ '^\d{8}$'
            THEN TO_DATE(TRIM(p.go_live_datum), 'YYYYMMDD')::TEXT
        WHEN TRIM(p.go_live_datum) ~ '\.'
            THEN TO_DATE(TRIM(p.go_live_datum), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(p.go_live_datum) ~ '/'
            THEN CASE
                WHEN SPLIT_PART(TRIM(p.go_live_datum), '/', 1) ~ '^\d+$'
                 AND SPLIT_PART(TRIM(p.go_live_datum), '/', 2) ~ '^\d+$'
                 AND SPLIT_PART(TRIM(p.go_live_datum), '/', 3) ~ '^\d{4}$'
                THEN TO_DATE(
                    SPLIT_PART(TRIM(p.go_live_datum), '/', 3) || '-' ||
                    LPAD(SPLIT_PART(TRIM(p.go_live_datum), '/', 1), 2, '0') || '-' ||
                    LPAD(SPLIT_PART(TRIM(p.go_live_datum), '/', 2), 2, '0'),
                    'YYYY-MM-DD'
                )::TEXT
                ELSE NULL
            END
        WHEN TRIM(p.go_live_datum) ~ '\-' AND LENGTH(TRIM(p.go_live_datum)) = 10
            THEN TO_DATE(TRIM(p.go_live_datum), 'YYYY-MM-DD')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    COALESCE(k.kundennummer, p.kunden_kennung) AS "Account__c",
    COALESCE(o.opp_kennung, p.opp_kennung_ref) AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
     '2000-01-01 00:00:00'::TEXT AS "CreatedDate",
     '2000-01-01 00:00:00'::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k
    ON TRIM(k.kundennummer) = TRIM(p.kunden_kennung)
LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} o
    ON TRIM(o.opp_kennung) = TRIM(p.opp_kennung_ref)