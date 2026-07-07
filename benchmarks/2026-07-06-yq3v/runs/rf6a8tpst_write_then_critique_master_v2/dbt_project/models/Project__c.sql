-- models/Project__c.sql

{{ config(materialized='table') }}

SELECT
    MD5(p.projekt_kennung) AS "Id",
    COALESCE(TRIM(p.projektname), 'Untitled Project') AS "Name",
    CASE UPPER(TRIM(p.projektstatus))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'AKTIV' THEN 'Active'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'PAUSIERT' THEN 'On Hold'
        WHEN 'PLANUNG' THEN 'In Planning'
        WHEN 'IN PLANUNG' THEN 'In Planning'
        WHEN 'ABGESCHLOSSEN' THEN 'Completed'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'CANCELLED' THEN 'Cancelled'
        WHEN 'STORNIERT' THEN 'Cancelled'
        ELSE 'In Planning' -- Changed NULL to a default enum value as per review
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' AND p.go_live_datum != '0000-00-00' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(p.kunden_kennung) AS "Account__c",
    MD5(p.opp_kennung_ref) AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS "CreatedDate",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p