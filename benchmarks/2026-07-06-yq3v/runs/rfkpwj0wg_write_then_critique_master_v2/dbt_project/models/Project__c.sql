{{ config(materialized='table') }}

SELECT
    T1.projekt_kennung AS "Id",
    COALESCE(T1.projektname, T1.projekt_kennung) AS "Name",
    CASE LOWER(TRIM(T1.projektstatus))
        WHEN 'active' THEN 'Active'
        WHEN 'aktiv' THEN 'Active'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'pausiert' THEN 'On Hold'
        WHEN 'planunung' THEN 'In Planning'
        WHEN 'planung' THEN 'In Planning'
        WHEN 'completed' THEN 'Completed'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'cancelled' THEN 'Cancelled'
        WHEN 'storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN T1.go_live_datum IS NULL OR TRIM(T1.go_live_datum) = '' OR T1.go_live_datum = '0000-00-00' THEN NULL
        WHEN T1.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(T1.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN T1.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(T1.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN T1.go_live_datum ~ '^\d{2}\/\d{2}\/\d{4}$' THEN TO_CHAR(TO_DATE(T1.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN T1.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(T1.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    T2.kundennummer AS "Account__c",
    T3.opp_kennung AS "Opportunity__c",
    T1.projekt_kennung AS "Legacy_Project_ID__c",
    '2023-01-01T00:00:00Z' AS "CreatedDate",
    '2023-01-01T00:00:00Z' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS T1
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS T2
ON
    T1.kunden_kennung = T2.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS T3
ON
    T1.opp_kennung_ref = T3.opp_kennung
