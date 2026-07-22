{{ config(materialized='table') }}

WITH projekte AS (
    SELECT
        projekt_kennung,
        projektname,
        projektstatus,
        go_live_datum,
        kunden_kennung,
        opp_kennung_ref
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
),

kunden AS (
    SELECT
        kundennummer,
        '001' || ENCODE(DIGEST(kundennummer, 'md5'), 'hex') AS account_id
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

opportunities AS (
    SELECT
        opp_kennung,
        '006' || ENCODE(DIGEST(opp_kennung, 'md5'), 'hex') AS opportunity_id
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
),

project_mapping AS (
    SELECT
        '701' || ENCODE(DIGEST(p.projekt_kennung, 'md5'), 'hex') AS Id,
        INITCAP(TRIM(p.projektname)) AS Name,
        CASE 
            WHEN UPPER(TRIM(p.projektstatus)) IN ('AKTIV', 'ACTIVE') THEN 'Active'
            WHEN UPPER(TRIM(p.projektstatus)) IN ('ABGESCHLOSSEN', 'COMPLETED') THEN 'Completed'
            WHEN UPPER(TRIM(p.projektstatus)) IN ('IN PLANUNG', 'IN PLANNING') THEN 'In Planning'
            WHEN UPPER(TRIM(p.projektstatus)) IN ('PAUSIERT', 'ON HOLD') THEN 'On Hold'
            WHEN UPPER(TRIM(p.projektstatus)) IN ('STORNIERT', 'CANCELLED') THEN 'Cancelled'
            ELSE NULL
        END AS "Project_Status__c",
        CASE 
            WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum
            WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
                TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN p.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN 
                TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN p.go_live_datum ~ '^\d{8}$' THEN 
                TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END AS "Go_Live_Date__c",
        kd.account_id AS Account__c,
        opp.opportunity_id AS Opportunity__c,
        p.projekt_kennung AS Legacy_Project_ID__c,
        TO_CHAR(NOW(), 'YYYY-MM-DD') AS CreatedDate,
        TO_CHAR(NOW(), 'YYYY-MM-DD') AS LastModifiedDate,
        0 AS IsDeleted
    FROM projekte p
    LEFT JOIN kunden kd ON p.kunden_kennung = kd.kundennummer
    LEFT JOIN opportunities opp ON p.opp_kennung_ref = opp.opp_kennung
)

SELECT
    Id,
    Name,
    "Project_Status__c",
    "Go_Live_Date__c",
    Account__c,
    Opportunity__c,
    Legacy_Project_ID__c,
    CreatedDate,
    LastModifiedDate,
    IsDeleted
FROM project_mapping
