-- models/Project__c.sql
{{ config(materialized='table') }}

SELECT
    MD5(projekt_kennung) AS "Id",
    COALESCE(TRIM(projektname), 'Unknown Project') AS "Name", -- Name is NOT NULL
    CASE
        WHEN LOWER(projektstatus) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(projektstatus) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(projektstatus) IN ('in planning', 'planung', 'in planung') THEN 'In Planning'
        WHEN LOWER(projektstatus) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(projektstatus) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL -- Default to NULL if not explicitly mapped
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum = '0000-00-00' THEN NULL -- Handle invalid date
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_datum::DATE -- YYYY-MM-DD
        WHEN go_live_datum ~ '^\d{8}$' THEN TO_DATE(go_live_datum, 'YYYYMMDD') -- YYYYMMDD
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live_datum, 'DD.MM.YYYY') -- DD.MM.YYYY (Not seen in sample, but for consistency if it appears)
        WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(go_live_datum, 'MM/DD/YYYY') -- M/D/YYYY or MM/DD/YYYY
        ELSE NULL
    END::TEXT AS "Go_Live_Date__c", -- Target is TEXT, output as ISO YYYY-MM-DD
    MD5(kunden_kennung) AS "Account__c", -- Use consistent Account Id generation
    MD5(opp_kennung_ref) AS "Opportunity__c", -- Use consistent Opportunity Id generation
    TRIM(projekt_kennung) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }}
