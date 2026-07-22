{{ config(materialized='table') }}
SELECT 
    mo.opp_kennung AS "Id",
    mo.titel AS "Name",
    CASE 
        WHEN UPPER(TRIM(mo.vertriebsphase)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(mo.vertriebsphase)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(mo.vertriebsphase)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(mo.vertriebsphase)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(mo.vertriebsphase)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(mo.vertriebsphase)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(mo.vertriebsphase)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(mo.vertriebsphase)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(mo.vertriebsphase)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(mo.vertriebsphase)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN mo.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN mo.zieldatum
        WHEN mo.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mo.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN mo.auftragswert ~ '^[0-9]+(\.[0-9]+)?$' THEN mo.auftragswert::DOUBLE PRECISION
        WHEN mo.auftragswert ~ '^[0-9]+,[0-9]+$' THEN REPLACE(mo.auftragswert, ',', '.')::DOUBLE PRECISION
        WHEN mo.auftragswert ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN REPLACE(REPLACE(mo.auftragswert, '.', ''), ',', '.')::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    CASE 
        WHEN mo.waehrungscode IS NOT NULL AND LENGTH(TRIM(mo.waehrungscode)) = 3 THEN UPPER(TRIM(mo.waehrungscode))
        ELSE NULL
    END AS "CurrencyIsoCode",
    MD5(mk.kundennummer) AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk ON mo.kunden_ref = mk.kundennummer