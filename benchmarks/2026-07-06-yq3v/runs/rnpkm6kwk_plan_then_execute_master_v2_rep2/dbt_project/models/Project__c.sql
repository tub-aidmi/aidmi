{{ config(materialized='table') }}

SELECT
    mp.projekt_kennung AS "Id",
    COALESCE(TRIM(mp.projektname), mp.projekt_kennung) AS "Name",
    CASE UPPER(TRIM(mp.projektstatus))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'IN PLANNING' THEN 'In Planning'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'CANCELLED' THEN 'Cancelled'
        ELSE 'In Planning' -- Default as per plan for NOT NULL enum
    END AS "Project_Status__c",
    CASE
        WHEN mp.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN mp.go_live_datum
        WHEN mp.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    mp.kunden_kennung AS "Account__c", -- Maps to Account.Id (master_kunden.kundennummer)
    mp.opp_kennung_ref AS "Opportunity__c", -- Maps to Opportunity.Id (master_opportunities.opp_kennung)
    mp.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
    ON mp.kunden_kennung = mk.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
    ON mp.opp_kennung_ref = mo.opp_kennung
