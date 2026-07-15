{{ config(materialized='table') }}

SELECT
    '006' || LEFT(opp_kennung, 15) AS "Id",

    titel AS "Name",

    CASE LOWER(TRIM(vertriebsphase))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'identifying decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        WHEN 'anbahnung' THEN 'Prospecting'
        WHEN 'qualifizierung' THEN 'Qualification'
        WHEN 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN 'wertversprechen' THEN 'Value Proposition'
        WHEN 'identifikation von entscheidungsträgern' THEN 'Id. Decision Makers'
        WHEN 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN 'angebot/preisanfrage' THEN 'Proposal/Price Quote'
        WHEN 'verhandlung/prüfung' THEN 'Negotiation/Review'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'verloren' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",

    CASE
        WHEN zieldatum IS NOT NULL AND zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(zieldatum, 'DD.MM.YYYY')::TEXT
        WHEN zieldatum IS NOT NULL AND zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN zieldatum
        WHEN zieldatum IS NOT NULL AND zieldatum ~ '^\d{8}$' THEN TO_DATE(zieldatum, 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "CloseDate",

    CASE
        WHEN auftragswert IS NOT NULL AND auftragswert ~ '[,.]'
            THEN CAST(REPLACE(REPLACE(REGEXP_REPLACE(auftragswert, '[^\d.,]', '', 'g'), '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN auftragswert IS NOT NULL AND auftragswert ~ '^\d+$'
            THEN REGEXP_REPLACE(auftragswert, '[^\d.,]', '', 'g')::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",

    waehrungscode AS "CurrencyIsoCode",

    '001' || LEFT(kunden_ref, 15) AS "AccountId",

    opp_kennung AS "Legacy_Opportunity_ID__c",

    CAST(CURRENT_TIMESTAMP AS TEXT) AS "CreatedDate",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "LastModifiedDate",

    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}