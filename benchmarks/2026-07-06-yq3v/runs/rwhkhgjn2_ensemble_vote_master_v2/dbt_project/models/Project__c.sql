{{ config(materialized='table') }}

SELECT
    p.projekt_kennung AS "Id",
    COALESCE(p.projektname, 'Unnamed Project') AS "Name",
    CASE
        WHEN LOWER(p.projektstatus) IN ('aktiv', 'active') THEN 'Active'
        WHEN LOWER(p.projektstatus) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN LOWER(p.projektstatus) IN ('in planung', 'planung', 'in planning') THEN 'In Planning'
        WHEN LOWER(p.projektstatus) IN ('in warteschleife', 'on hold') THEN 'On Hold'
        WHEN LOWER(p.projektstatus) IN ('abgesagt', 'cancelled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum = '0000-00-00' THEN NULL
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum -- YYYY-MM-DD
        WHEN p.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    CONCAT('ACC-', k.kundennummer) AS "Account__c",
    CONCAT('OPP-', o.opp_kennung) AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
    ON p.kunden_kennung = k.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o
    ON p.opp_kennung_ref = o.opp_kennung
