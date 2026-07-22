{{ config(materialized='table') }}

SELECT
    gen_random_uuid() AS "Id",
    COALESCE(TRIM(p.projektname), 'Unnamed Project') AS "Name",
    CASE UPPER(TRIM(p.projektstatus))
        WHEN 'AKTIV' THEN 'Active'
        WHEN 'ABGESCHLOSSEN' THEN 'Completed'
        WHEN 'IN PLANUNG' THEN 'In Planning'
        WHEN 'IN HALT' THEN 'On Hold'
        WHEN 'STORNIERT' THEN 'Cancelled'
        ELSE 'In Planning'
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(p.go_live_datum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(p.go_live_datum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(p.go_live_datum) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(p.go_live_datum), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(p.go_live_datum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(p.go_live_datum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    a."Id" AS "Account__c",
    o."Id" AS "Opportunity__c",
    TRIM(p.projekt_kennung) AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p
LEFT JOIN
    {{ ref('Account') }} AS a ON TRIM(p.kunden_kennung) = a."Legacy_Customer_ID__c"
LEFT JOIN
    {{ ref('Opportunity') }} AS o ON TRIM(p.opp_kennung_ref) = o."Legacy_Opportunity_ID__c"
