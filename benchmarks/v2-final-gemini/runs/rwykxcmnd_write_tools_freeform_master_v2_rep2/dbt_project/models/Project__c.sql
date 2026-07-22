{{ config(materialized='table') }}

WITH cleaned_projekte AS (
    SELECT
        projekt_kennung,
        projektname,
        projektstatus,
        go_live_datum,
        kunden_kennung,
        opp_kennung_ref,
        -- Defaulting CreatedDate and LastModifiedDate as source doesn't provide
        CAST(CURRENT_TIMESTAMP AS TEXT) AS created_date,
        CAST(CURRENT_TIMESTAMP AS TEXT) AS last_modified_date
    FROM
        {{ source('fixture_master_v2_src', 'master_projekte') }}
)
SELECT
    MD5(projekt_kennung) AS "Id",
    COALESCE(projektname, 'Unknown Project') AS "Name", -- Name is NOT NULL
    CASE
        WHEN LOWER(projektstatus) = 'active' THEN 'Active'
        WHEN LOWER(projektstatus) = 'completed' THEN 'Completed'
        WHEN LOWER(projektstatus) = 'in planning' THEN 'In Planning'
        WHEN LOWER(projektstatus) = 'on hold' THEN 'On Hold'
        WHEN LOWER(projektstatus) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    TO_CHAR(
        CASE
            WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(go_live_datum, 'YYYY-MM-DD')
            WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live_datum, 'DD.MM.YYYY')
            WHEN go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(go_live_datum, 'MM/DD/YYYY')
            ELSE NULL
        END,
    'YYYY-MM-DD') AS "Go_Live_Date__c",
    MD5(kunden_kennung) AS "Account__c", -- Account__c is derived from kunden_kennung
    MD5(opp_kennung_ref) AS "Opportunity__c", -- Opportunity__c is derived from opp_kennung_ref
    projekt_kennung AS "Legacy_Project_ID__c",
    created_date AS "CreatedDate",
    last_modified_date AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_projekte
