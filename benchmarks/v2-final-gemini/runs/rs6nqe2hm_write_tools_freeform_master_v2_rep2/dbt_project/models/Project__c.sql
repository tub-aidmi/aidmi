-- models/Project__c.sql
{{ config(materialized='table') }}

SELECT
    proj.projekt_kennung AS "Id",
    COALESCE(proj.projektname, 'Unknown Project Name') AS "Name", -- Name is NOT NULL
    CASE
        WHEN LOWER(proj.projektstatus) = 'active' THEN 'Active'
        WHEN LOWER(proj.projektstatus) = 'completed' THEN 'Completed'
        WHEN LOWER(proj.projektstatus) = 'in planning' THEN 'In Planning'
        WHEN LOWER(proj.projektstatus) = 'on hold' THEN 'On Hold'
        WHEN LOWER(proj.projektstatus) = 'cancelled' THEN 'Cancelled'
        ELSE 'In Planning' -- Fallback for NOT NULL enum
    END AS "Project_Status__c",
    COALESCE(
        TO_CHAR(TO_DATE(proj.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(proj.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(proj.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(proj.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
    ) AS "Go_Live_Date__c",
    mak.kundennummer AS "Account__c", -- Join on kunden_kennung and kundennummer
    mop.opp_kennung AS "Opportunity__c", -- Join on opp_kennung_ref and opp_kennung
    proj.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS proj
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mak
    ON proj.kunden_kennung = mak.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mop
    ON proj.opp_kennung_ref = mop.opp_kennung
