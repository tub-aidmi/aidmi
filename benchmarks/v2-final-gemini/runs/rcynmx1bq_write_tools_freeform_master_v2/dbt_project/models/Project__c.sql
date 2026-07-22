{{ config(materialized='table') }}

SELECT
    MD5(TRIM(projekt_kennung)) AS "Id",
    COALESCE(TRIM(projektname), 'Unknown Project') AS "Name",
    CASE LOWER(TRIM(projektstatus))
        WHEN 'aktiv' THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'storniert' THEN 'Cancelled'
        ELSE 'In Planning' -- Default for NOT NULL target
    END AS "Project_Status__c",
    COALESCE(
        CAST(CASE
            WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_datum -- YYYY-MM-DD
            WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END AS text),
        CURRENT_DATE::text -- Default for NOT NULL target
    ) AS "Go_Live_Date__c",
    MD5(TRIM(kunden_kennung)) AS "Account__c",
    MD5(TRIM(opp_kennung_ref)) AS "Opportunity__c",
    TRIM(projekt_kennung) AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }}