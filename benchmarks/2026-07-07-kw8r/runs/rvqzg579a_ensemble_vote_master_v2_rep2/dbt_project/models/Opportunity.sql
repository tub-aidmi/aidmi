{{ config(materialized='table') }}

SELECT
    mo.opp_kennung AS "Id",
    COALESCE(NULLIF(TRIM(mo.titel), ''), 'Untitled Opportunity') AS "Name",
    CASE
        WHEN TRIM(mo.vertriebsphase) = 'Prospecting' THEN 'Prospecting'
        WHEN TRIM(mo.vertriebsphase) = 'Qualification' THEN 'Qualification'
        WHEN TRIM(mo.vertriebsphase) = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN TRIM(mo.vertriebsphase) = 'Value Proposition' THEN 'Value Proposition'
        WHEN TRIM(mo.vertriebsphase) = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN TRIM(mo.vertriebsphase) = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN TRIM(mo.vertriebsphase) = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(mo.vertriebsphase) = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN TRIM(mo.vertriebsphase) = 'Closed Won' THEN 'Closed Won'
        WHEN TRIM(mo.vertriebsphase) = 'Closed Lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN mo.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN mo.zieldatum
        WHEN mo.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mo.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN mo.auftragswert ~ '^[\d\.]+,\d+$' THEN
            CAST(REPLACE(REPLACE(mo.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN mo.auftragswert ~ '^[\d]+(\.\d+)?$' THEN CAST(mo.auftragswert AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    NULLIF(TRIM(mo.waehrungscode), '') AS "CurrencyIsoCode",
    mk.kundennummer AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} mk
    ON mo.kunden_ref = mk.kundennummer