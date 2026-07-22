{{ config(materialized='table') }}

SELECT
    LEFT('p01' || LPAD(TRIM(p.projekt_kennung), 18, '0'), 21) AS "Id",
    COALESCE(INITCAP(TRIM(p.projektname)), 'Unnamed Project') AS "Name",
    CASE UPPER(TRIM(p.projektstatus))
        WHEN 'AKTIV' THEN 'Active'
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'FERTIG' THEN 'Completed'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'ERLEDIGT' THEN 'Completed'
        WHEN 'GEPLANT' THEN 'In Planning'
        WHEN 'IN PLANNING' THEN 'In Planning'
        WHEN 'PAUSIERT' THEN 'On Hold'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'GESPEICHERT' THEN 'On Hold'
        WHEN 'ABBRECHEN' THEN 'Cancelled'
        WHEN 'CANCELLED' THEN 'Cancelled'
        WHEN 'GELOESCHT' THEN 'Cancelled'
        WHEN 'DELETED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum IS NOT NULL
             AND TRIM(p.go_live_datum) <> ''
             AND TRIM(p.go_live_datum) NOT IN ('0000-00-00', '0001-01-01')
            THEN CASE
                WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$'
                    THEN TO_CHAR(TO_DATE(TRIM(p.go_live_datum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
                WHEN p.go_live_datum ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$'
                    THEN CAST(p.go_live_datum AS DATE)::TEXT
            END
        ELSE NULL
    END AS "Go_Live_Date__c",
    '001' || LPAD(TRIM(p.kunden_kennung), 15, '0') AS "Account__c",
    '006' || LPAD(TRIM(p.opp_kennung_ref), 9, '0') AS "Opportunity__c",
    TRIM(p.projekt_kennung) AS "Legacy_Project_ID__c",
    CAST(NOW() AS TEXT) AS "CreatedDate",
    CAST(NOW() AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p