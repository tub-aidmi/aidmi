{{ config(materialized='table') }}

WITH project_date_parsed AS (
    SELECT
        projekt_kennung,
        CASE
            WHEN go_live_datum ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END AS parsed_go_live_date
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
)

SELECT
    p.projekt_kennung AS "Id",
    TRIM(p.projektname) AS "Name",
    CASE
        WHEN p.projektstatus = 'Aktiv' THEN 'Active'
        WHEN p.projektstatus = 'Abgeschlossen' THEN 'Completed'
        WHEN p.projektstatus = 'In Planung' THEN 'In Planning'
        WHEN p.projektstatus = 'Pausiert' THEN 'On Hold'
        WHEN p.projektstatus = 'Storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    pd.parsed_go_live_date AS "Go_Live_Date__c",
    k.kundennummer AS "Account__c",
    o.opp_kennung AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k
    ON p.kunden_kennung = k.kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} o
    ON p.opp_kennung_ref = o.opp_kennung
LEFT JOIN project_date_parsed pd
    ON p.projekt_kennung = pd.projekt_kennung