{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        projekt_kennung,
        projektname,
        projektstatus,
        go_live_datum,
        kunden_kennung,
        opp_kennung_ref
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
)
SELECT
    MD5(projekt_kennung) AS "Id",
    COALESCE(projektname, 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(projektstatus) = 'active' THEN 'Active'
        WHEN LOWER(projektstatus) = 'completed' THEN 'Completed'
        WHEN LOWER(projektstatus) = 'in planning' THEN 'In Planning'
        WHEN LOWER(projektstatus) = 'on hold' THEN 'On Hold'
        WHEN LOWER(projektstatus) = 'cancelled' THEN 'Cancelled'
        ELSE 'Active' -- Fallback for NOT NULL target
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{2}\/\d{2}\/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE TO_CHAR(NOW(), 'YYYY-MM-DD') -- Fallback for NOT NULL
    END AS "Go_Live_Date__c",
    MD5(kunden_kennung) AS "Account__c",
    MD5(opp_kennung_ref) AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_data
