{{ config(materialized='table') }}

SELECT
    'OPP_' || opp_kennung AS "Id",
    titel AS "Name",
    CASE 
        WHEN UPPER(vertriebsphase) IN ('PROSPEKTIERUNG', 'PROSPECTING') THEN 'Prospecting'
        WHEN UPPER(vertriebsphase) IN ('QUALIFIZIERUNG', 'QUALIFICATION') THEN 'Qualification'
        WHEN UPPER(vertriebsphase) IN ('BEDARFSANALYSE', 'NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(vertriebsphase) IN ('WERTVORSCHLAG', 'VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(vertriebsphase) IN ('ENTSCHEIDUNGSTRÄGER IDENTIFIZIEREN', 'ID. DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(vertriebsphase) IN ('WAHRNEHMUNGSANALYSE', 'PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(vertriebsphase) IN ('ANGEBOT/PREISANGEBOT', 'PROPOSAL/PRICE QUOTE') THEN 'Proposal/Price Quote'
        WHEN UPPER(vertriebsphase) IN ('VERHANDLUNG/ÜBERPRÜFUNG', 'NEGOTIATION/REVIEW') THEN 'Negotiation/Review'
        WHEN UPPER(vertriebsphase) IN ('GESCHLOSSEN GEWONNEN', 'CLOSED WON') THEN 'Closed Won'
        WHEN UPPER(vertriebsphase) IN ('GESCHLOSSEN VERLOREN', 'CLOSED LOST') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN zieldatum ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN auftragswert ~ '^[\d]+[.,]?[\d]*$' THEN 
            REGEXP_REPLACE(REGEXP_REPLACE(auftragswert, '\.', ''), ',', '.')::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    waehrungscode AS "CurrencyIsoCode",
    'ACC_' || kunden_ref AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
