{{ config(materialized='table') }}

SELECT
    MD5(mp.projekt_kennung) AS "Id",
    COALESCE(mp.projektname, 'Unnamed Project') AS "Name",
    CASE
        WHEN LOWER(mp.projektstatus) LIKE '%active%' THEN 'Active'
        WHEN LOWER(mp.projektstatus) LIKE '%complete%' THEN 'Completed'
        WHEN LOWER(mp.projektstatus) LIKE '%plan%' THEN 'In Planning'
        WHEN LOWER(mp.projektstatus) LIKE '%hold%' THEN 'On Hold'
        WHEN LOWER(mp.projektstatus) LIKE '%cancel%' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN mp.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN mp.go_live_datum
        WHEN mp.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(mk.kundennummer) AS "Account__c",
    MD5(mo.opp_kennung) AS "Opportunity__c",
    mp.projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} mp
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} mk
ON
    mp.kunden_kennung = mk.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
ON
    mp.opp_kennung_ref = mo.opp_kennung
