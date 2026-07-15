{{ config(materialized='table') }}
SELECT 
    'OPP-' || mo.opp_kennung AS "Id",
    mo.titel AS "Name",
    CASE 
        WHEN mo.vertriebsphase ILIKE '%prospecting%' THEN 'Prospecting'
        WHEN mo.vertriebsphase ILIKE '%qualification%' THEN 'Qualification'
        WHEN mo.vertriebsphase ILIKE '%needs analysis%' THEN 'Needs Analysis'
        WHEN mo.vertriebsphase ILIKE '%value proposition%' THEN 'Value Proposition'
        WHEN mo.vertriebsphase ILIKE '%id. decision makers%' THEN 'Id. Decision Makers'
        WHEN mo.vertriebsphase ILIKE '%perception analysis%' THEN 'Perception Analysis'
        WHEN mo.vertriebsphase ILIKE '%proposal%' OR mo.vertriebsphase ILIKE '%price quote%' THEN 'Proposal/Price Quote'
        WHEN mo.vertriebsphase ILIKE '%negotiation%' OR mo.vertriebsphase ILIKE '%review%' THEN 'Negotiation/Review'
        WHEN mo.vertriebsphase ILIKE '%closed won%' THEN 'Closed Won'
        WHEN mo.vertriebsphase ILIKE '%closed lost%' THEN 'Closed Lost'
        ELSE NULL 
    END AS "StageName",
    CASE 
        WHEN mo.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN mo.zieldatum
        WHEN mo.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mo.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL 
    END AS "CloseDate",
    CASE 
        WHEN mo.auftragswert ~ '[0-9]' THEN 
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(mo.auftragswert, '[^0-9.,]', '', 'g'),
                    '\.', '', 'g'
                ),
                ',', '.', 'g'
            )::DOUBLE PRECISION
        ELSE NULL 
    END AS "Amount",
    mo.waehrungscode AS "CurrencyIsoCode",
    'ACC-' || mk.kundennummer AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk ON mo.kunden_ref = mk.kundennummer