{{ config(materialized='table') }}

SELECT
    projekt_kennung AS "Id",
    projektname AS "Name",
    CASE
        WHEN UPPER(TRIM(projektstatus)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(projektstatus)) IN ('COMPLETED', 'ABGESCHLOSSEN') THEN 'Completed'
        WHEN UPPER(TRIM(projektstatus)) IN ('IN PLANNING', 'IN PLANUNG', 'PLANUNG') THEN 'In Planning'
        WHEN UPPER(TRIM(projektstatus)) IN ('ON HOLD', 'PAUSIERT') THEN 'On Hold'
        WHEN UPPER(TRIM(projektstatus)) IN ('CANCELLED', 'STORNIERT') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum IS NULL OR TRIM(go_live_datum) = '' THEN NULL
        WHEN go_live_datum ~ '^0000-00-00$' THEN NULL
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(go_live_datum, 'YYYY-MM-DD')::TEXT
        WHEN go_live_datum ~ '^\d{8}$' THEN TO_DATE(go_live_datum, 'YYYYMMDD')::TEXT
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live_datum, 'DD.MM.YYYY')::TEXT
        WHEN go_live_datum ~ '^\d+/\d+/\d{4}$' THEN 
            CASE 
                WHEN SPLIT_PART(go_live_datum, '/', 3)::INTEGER > 100 THEN 
                    TO_DATE(go_live_datum, 'MM/DD/YYYY')::TEXT
                ELSE NULL
            END
        ELSE NULL
    END AS "Go_Live_Date__c",
    -- Account Id: join with master_kunden to resolve CUST-M prefix → SFDC-style 001-prefixed Id
    CASE
        WHEN mk.kundennummer IS NOT NULL
        THEN '001' || LPAD(CAST(REGEXP_REPLACE(mk.kundennummer, '[^0-9]', '', '') AS INTEGER), 9, '0')
        ELSE NULL
    END AS "Account__c",
    opp_kennung_ref AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk 
    ON p.kunden_kennung = mk.kundennummer