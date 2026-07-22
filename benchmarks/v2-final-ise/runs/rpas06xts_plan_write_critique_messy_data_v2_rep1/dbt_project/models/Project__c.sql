{{ config(materialized='table') }}
WITH project_status_mapping AS (
    SELECT 'Aktiv' AS source_status, 'Active' AS target_status UNION ALL
    SELECT 'Planung', 'In Planning' UNION ALL
    SELECT 'In Planung', 'In Planning' UNION ALL
    SELECT 'completed', 'Completed' UNION ALL
    SELECT 'Storniert', 'Cancelled' UNION ALL
    SELECT 'Active', 'Active' UNION ALL
    SELECT 'Completed', 'Completed' UNION ALL
    SELECT 'On Hold', 'On Hold' UNION ALL
    SELECT 'Cancelled', 'Cancelled' UNION ALL
    SELECT 'Abgeschlossen', 'Completed' UNION ALL
    SELECT 'Pausiert', 'On Hold' UNION ALL
    SELECT 'In Bearbeitung', 'Active'
),
parsed_projects AS (
    SELECT
        p.id,
        TRIM(p.name) AS name,
        p.project_status__c,
        p.go_live_date__c,
        p.account__c,
        p.opportunity__c,
        COALESCE(
            CASE
                WHEN p.go_live_date__c ~ '^\d{8}$' THEN TO_DATE(p.go_live_date__c, 'YYYYMMDD')
                WHEN p.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(p.go_live_date__c, 'YYYY-MM-DD')
                WHEN p.go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(p.go_live_date__c, 'MM/DD/YYYY')
                WHEN p.go_live_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(p.go_live_date__c, 'DD.MM.YYYY')
                ELSE NULL
            END,
            NULL
        ) AS parsed_go_live_date
    FROM {{ source('fixture_messy_data_v2_src', 'project__c') }} p
),
status_mapping AS (
    SELECT
        p.id,
        COALESCE(
            (
                SELECT target_status
                FROM project_status_mapping
                WHERE LOWER(TRIM(source_status)) = LOWER(TRIM(p.project_status__c))
                LIMIT 1
            ),
            'In Planning'
        ) AS mapped_status
    FROM parsed_projects p
)
SELECT
    p.id AS "Id",
    COALESCE(INITCAP(TRIM(p.name)), 'Unknown Project') AS "Name",
    s.mapped_status AS "Project_Status__c",
    CASE
        WHEN p.parsed_go_live_date IS NOT NULL
        THEN TO_CHAR(p.parsed_go_live_date, 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM parsed_projects p
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a ON p.account__c = a.id
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'opportunity') }} o ON p.opportunity__c = o.id
LEFT JOIN status_mapping s ON p.id = s.id