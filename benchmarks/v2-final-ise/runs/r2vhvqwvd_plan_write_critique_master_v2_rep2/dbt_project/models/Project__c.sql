{{ config(materialized='table') }}

SELECT
    CAST(m_proj.projekt_kennung AS text) AS "Id",
    m_proj.projektname AS "Name",
    CASE
        WHEN UPPER(TRIM(m_proj.projektstatus)) = 'AKTIV' THEN 'Active'
        WHEN UPPER(TRIM(m_proj.projektstatus)) = 'ABGESCHLOSSEN' THEN 'Completed'
        WHEN UPPER(TRIM(m_proj.projektstatus)) = 'IN_PLANUNG' THEN 'In Planning'
        WHEN UPPER(TRIM(m_proj.projektstatus)) = 'PAUSIERT' THEN 'On Hold'
        WHEN UPPER(TRIM(m_proj.projektstatus)) = 'STORNIERT' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN m_proj.go_live_datum IS NOT NULL
            AND (m_proj.go_live_datum ~ '^\d{8}$' OR m_proj.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$') THEN
            CASE
                WHEN m_proj.go_live_datum ~ '^\d{8}$' THEN TO_DATE(m_proj.go_live_datum, 'YYYYMMDD')::TEXT
                ELSE TO_DATE(m_proj.go_live_datum, 'DD.MM.YYYY')::TEXT
            END
        ELSE NULL
    END AS "Go_Live_Date__c",
    m_ku.kundennummer AS "Account__c",
    m_opp.opp_kennung AS "Opportunity__c",
    CAST(m_proj.projekt_kennung AS text) AS "Legacy_Project_ID__c",
    '2024-01-01 00:00:00' AS "CreatedDate",
    '2024-01-01 00:00:00' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} m_proj
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} m_ku
    ON LOWER(TRIM(m_proj.kunden_kennung)) = LOWER(TRIM(m_ku.kundennummer))
LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} m_opp
    ON LOWER(TRIM(m_proj.opp_kennung_ref)) = LOWER(TRIM(m_opp.opp_kennung))