-- models/Project__c.sql

{{ config(materialized='table') }}

SELECT
    TRIM(projekt_kennung) AS "Id",
    COALESCE(TRIM(projektname), 'Unknown Project') AS "Name",
    CASE UPPER(TRIM(projektstatus))
        WHEN 'AKTIV' THEN 'Active'
        WHEN 'ABGESCHLOSSEN' THEN 'Completed'
        WHEN 'IN PLANUNG' THEN 'In Planning'
        WHEN 'INHALT' THEN 'On Hold' -- Assuming 'Inhalt' or similar is 'On Hold' based on common project statuses
        WHEN 'STORNIERT' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    COALESCE(
        TO_CHAR(CASE WHEN TRIM(go_live_datum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(go_live_datum), 'YYYY-MM-DD') ELSE NULL END, 'YYYY-MM-DD'),
        TO_CHAR(CASE WHEN TRIM(go_live_datum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(go_live_datum), 'DD.MM.YYYY') ELSE NULL END, 'YYYY-MM-DD'),
        TO_CHAR(CASE WHEN TRIM(go_live_datum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(go_live_datum), 'MM/DD/YYYY') ELSE NULL END, 'YYYY-MM-DD'),
        NULL -- Allow NULL as target column is not NOT NULL
    ) AS "Go_Live_Date__c",
    TRIM(kunden_kennung) AS "Account__c",
    TRIM(opp_kennung_ref) AS "Opportunity__c",
    TRIM(projekt_kennung) AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }}
