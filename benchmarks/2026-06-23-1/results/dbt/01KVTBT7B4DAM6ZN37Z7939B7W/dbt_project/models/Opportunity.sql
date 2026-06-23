{{ config(materialized='table') }}

SELECT
    opp_kennung AS Id,
    COALESCE(titel, 'Untitled Opportunity') AS Name,
    CASE 
        WHEN LOWER(vertriebsphase) = 'gewonnen' THEN 'Closed Won'
        WHEN LOWER(vertriebsphase) = 'verloren' THEN 'Closed Lost'
        WHEN LOWER(vertriebsphase) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(vertriebsphase) = 'qualification' THEN 'Qualification'
        WHEN LOWER(vertriebsphase) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(vertriebsphase) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(vertriebsphase) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(vertriebsphase) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(vertriebsphase) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(vertriebsphase) = 'negotiation/review' THEN 'Negotiation/Review'
        ELSE 'Prospecting'
    END AS StageName,
    COALESCE(
        CASE 
            WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN zieldatum
            WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1970-01-01'
    ) AS CloseDate,
    CASE 
        WHEN auftragswert ~ '^[\d.]+$' THEN auftragswert::double precision
        WHEN auftragswert ~ '^[A-Z]{3} [\d.]+$' THEN 
            REGEXP_REPLACE(auftragswert, '^[A-Z]{3} ', '')::double precision
        ELSE NULL
    END AS Amount,
    CASE 
        WHEN waehrungscode IN ('EUR', '€') THEN 'EUR'
        WHEN waehrungscode IN ('USD', 'Dollar', '$') THEN 'USD'
        WHEN waehrungscode IN ('CHF') THEN 'CHF'
        WHEN waehrungscode IN ('GBP', '£') THEN 'GBP'
        ELSE 'EUR'
    END AS CurrencyIsoCode,
    kunden_ref AS AccountId,
    opp_kennung AS Legacy_Opportunity_ID__c,
    CURRENT_TIMESTAMP::text AS CreatedDate,
    CURRENT_TIMESTAMP::text AS LastModifiedDate,
    0 AS IsDeleted

FROM {{ source('fixture_master_src', 'master_opportunities') }}
