-- noinspection SqlNoDataSourceInspectionForFile

{{ config(materialized='table') }}

SELECT
    MD5(p.projekt_kennung) AS "Id",
    COALESCE(p.projektname, 'Unknown Project') AS "Name",
    CASE
        WHEN p.projektstatus = 'Abgeschlossen' THEN 'Completed'
        WHEN p.projektstatus = 'Aktiv' THEN 'Active'
        WHEN p.projektstatus = 'In Planung' THEN 'In Planning'
        WHEN p.projektstatus = 'Pausiert' THEN 'On Hold'
        WHEN p.projektstatus = 'Storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    a."Id" AS "Account__c",
    opp."Id" AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} AS p
LEFT JOIN {{ ref('Account') }} AS a
    ON p.kunden_kennung = a."Legacy_Customer_ID__c"
LEFT JOIN {{ ref('Opportunity') }} AS opp
    ON p.opp_kennung_ref = opp."Legacy_Opportunity_ID__c"