-- depends_on: {{ source('fixture_master_v2_src', 'master_projekte') }}
{{ config(materialized='table') }}

SELECT
    mp.projekt_kennung AS "Id",
    COALESCE(mp.projektname, 'Unknown Project ' || mp.projekt_kennung) AS "Name",
    CASE
        WHEN LOWER(mp.projektstatus) IN ('aktiv', 'active') THEN 'Active'
        WHEN LOWER(mp.projektstatus) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN LOWER(mp.projektstatus) IN ('in planung', 'in_planning') THEN 'In Planning'
        WHEN LOWER(mp.projektstatus) IN ('auf eis', 'on_hold') THEN 'On Hold'
        WHEN LOWER(mp.projektstatus) IN ('abgesagt', 'cancelled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN mp.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD') -- Assuming YYYYMMDD format for 8 digits
        ELSE NULL
    END AS "Go_Live_Date__c",
    mp.kunden_kennung AS "Account__c",
    mp.opp_kennung_ref AS "Opportunity__c",
    mp.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp