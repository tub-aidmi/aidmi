{{ config(materialized='table') }}

SELECT
    projekt_kennung AS "Id",
    COALESCE(TRIM(projektname), projekt_kennung) AS "Name", -- Name is NOT NULL
    CASE
        WHEN LOWER(TRIM(projektstatus)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(projektstatus)) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(TRIM(projektstatus)) IN ('in planning', 'in planung') THEN 'In Planning'
        WHEN LOWER(TRIM(projektstatus)) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(projektstatus)) IN ('cancelled', 'abgebrochen') THEN 'Cancelled'
        ELSE 'In Planning' -- Default to a valid status if not matched, as it's NOT NULL
    END AS "Project_Status__c",
    COALESCE(
        TO_CHAR(
            TO_DATE(TRIM(go_live_datum), 'YYYY-MM-DD'), 'YYYY-MM-DD'
        ),
        TO_CHAR(
            TO_DATE(TRIM(go_live_datum), 'DD.MM.YYYY'), 'YYYY-MM-DD'
        ),
        TO_CHAR(
            TO_DATE(TRIM(go_live_datum), 'YYYYMMDD'), 'YYYY-MM-DD'
        ),
        NULL
    ) AS "Go_Live_Date__c",
    kunden_kennung AS "Account__c", -- Maps to Account.Id
    opp_kennung_ref AS "Opportunity__c", -- Maps to Opportunity.Id
    projekt_kennung AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
