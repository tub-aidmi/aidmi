{{ config(materialized='table') }}

WITH opportunities AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
),

kunden AS (
    SELECT
        kundennummer,
        '001' || ENCODE(DIGEST(kundennummer, 'md5'), 'hex') AS account_id
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

opportunity_mapping AS (
    SELECT
        '006' || ENCODE(DIGEST(o.opp_kennung, 'md5'), 'hex') AS Id,
        INITCAP(TRIM(o.titel)) AS Name,
        CASE 
            WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PROSPEKTIERUNG', 'PROSPECTING') THEN 'Prospecting'
            WHEN UPPER(TRIM(o.vertriebsphase)) IN ('QUALIFIZIERUNG', 'QUALIFICATION') THEN 'Qualification'
            WHEN UPPER(TRIM(o.vertriebsphase)) IN ('BEDARFSANALYSE', 'NEEDS ANALYSIS') THEN 'Needs Analysis'
            WHEN UPPER(TRIM(o.vertriebsphase)) IN ('WERTVERSPRECHEN', 'VALUE PROPOSITION') THEN 'Value Proposition'
            WHEN UPPER(TRIM(o.vertriebsphase)) IN ('ENTSCHEIDUNGSTRÄGER IDENTIFIZIEREN', 'ID. DECISION MAKERS') THEN 'Id. Decision Makers'
            WHEN UPPER(TRIM(o.vertriebsphase)) IN ('WAHRNEHMUNGSANALYSE', 'PERCEPTION ANALYSIS') THEN 'Perception Analysis'
            WHEN UPPER(TRIM(o.vertriebsphase)) IN ('ANGEBOT/PREISANGEBOT', 'PROPOSAL/PRICE QUOTE') THEN 'Proposal/Price Quote'
            WHEN UPPER(TRIM(o.vertriebsphase)) IN ('VERHANDLUNG/ÜBERPRÜFUNG', 'NEGOTIATION/REVIEW') THEN 'Negotiation/Review'
            WHEN UPPER(TRIM(o.vertriebsphase)) IN ('GESCHLOSSEN GEWONNEN', 'CLOSED WON') THEN 'Closed Won'
            WHEN UPPER(TRIM(o.vertriebsphase)) IN ('GESCHLOSSEN VERLOREN', 'CLOSED LOST') THEN 'Closed Lost'
            ELSE NULL
        END AS "StageName",
        CASE 
            WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN o.zieldatum
            WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
                TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN o.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN 
                TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN o.zieldatum ~ '^\d{8}$' THEN 
                TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END AS CloseDate,
        CASE 
            WHEN o.auftragswert ~ '^[0-9]+\.[0-9]{3},[0-9]{2}$' THEN 
                CAST(REPLACE(REPLACE(o.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
            WHEN o.auftragswert ~ '^[0-9]+,[0-9]{2}$' THEN 
                CAST(REPLACE(o.auftragswert, ',', '.') AS DOUBLE PRECISION)
            WHEN o.auftragswert ~ '^[0-9]+\.[0-9]{2}$' THEN 
                CAST(o.auftragswert AS DOUBLE PRECISION)
            WHEN o.auftragswert ~ '^[0-9]+$' THEN 
                CAST(o.auftragswert AS DOUBLE PRECISION)
            ELSE NULL
        END AS Amount,
        NULLIF(TRIM(o.waehrungscode), '') AS CurrencyIsoCode,
        kd.account_id AS AccountId,
        o.opp_kennung AS Legacy_Opportunity_ID__c,
        TO_CHAR(NOW(), 'YYYY-MM-DD') AS CreatedDate,
        TO_CHAR(NOW(), 'YYYY-MM-DD') AS LastModifiedDate,
        0 AS IsDeleted
    FROM opportunities o
    LEFT JOIN kunden kd ON o.kunden_ref = kd.kundennummer
)

SELECT
    Id,
    Name,
    "StageName",
    CloseDate,
    Amount,
    CurrencyIsoCode,
    AccountId,
    Legacy_Opportunity_ID__c,
    CreatedDate,
    LastModifiedDate,
    IsDeleted
FROM opportunity_mapping
