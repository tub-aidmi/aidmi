{% config(materialized='table') %}

SELECT
    opp.opp_kennung AS Id,
    COALESCE(NULLIF(TRIM(opp.titel), ''), 'Untitled Opportunity') AS Name,
    CASE
        WHEN UPPER(TRIM(opp.vertriebsphase)) = 'GEWONNEN' THEN 'Closed Won'
        WHEN UPPER(TRIM(opp.vertriebsphase)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(opp.vertriebsphase)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(opp.vertriebsphase)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(opp.vertriebsphase)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(opp.vertriebsphase)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(opp.vertriebsphase)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(opp.vertriebsphase)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(opp.vertriebsphase)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(opp.vertriebsphase)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS StageName,
    COALESCE(NULLIF(TRIM(opp.zieldatum), 'N/A'), NULLIF(TRIM(opp.zieldatum), '')) AS CloseDate,
    CASE
        WHEN opp.auftragswert ~ '^[0-9]+(\.[0-9]+)?$' THEN CAST(opp.auftragswert AS DOUBLE PRECISION)
        WHEN opp.auftragswert ~ '^[A-Z]{3} [0-9]+(\.[0-9]+)?$' THEN CAST(REGEXP_REPLACE(opp.auftragswert, '^[A-Z]{3} ', '') AS DOUBLE PRECISION)
        WHEN opp.auftragswert ~ '^[€$£¥]?[0-9]+(\.[0-9]+)?$' THEN CAST(REGEXP_REPLACE(opp.auftragswert, '[^0-9.]', '') AS DOUBLE PRECISION)
        ELSE NULL
    END AS Amount,
    CASE
        WHEN UPPER(TRIM(opp.waehrungscode)) IN ('EUR', '€') THEN 'EUR'
        WHEN UPPER(TRIM(opp.waehrungscode)) IN ('CHF') THEN 'CHF'
        WHEN UPPER(TRIM(opp.waehrungscode)) IN ('USD', 'DOLLAR', '$') THEN 'USD'
        WHEN UPPER(TRIM(opp.waehrungscode)) IN ('GBP', '£') THEN 'GBP'
        WHEN UPPER(TRIM(opp.waehrungscode)) IN ('JPY', '¥') THEN 'JPY'
        ELSE NULL
    END AS CurrencyIsoCode,
    cust.kundennummer AS AccountId,
    opp.opp_kennung AS Legacy_Opportunity_ID__c,
    NULL::text AS CreatedDate,
    NULL::text AS LastModifiedDate,
    0 AS IsDeleted
FROM {{ source('fixture_master_src', 'master_opportunities') }} opp
INNER JOIN {{ source('fixture_master_src', 'master_kunden') }} cust
    ON opp.kunden_ref = cust.kundennummer