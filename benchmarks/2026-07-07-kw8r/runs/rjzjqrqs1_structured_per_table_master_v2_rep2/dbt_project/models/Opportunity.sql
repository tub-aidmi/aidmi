{{ config(materialized='table') }}

SELECT 
    mo.opp_kennung AS "Id",
    mo.titel AS "Name",
    CASE 
        WHEN UPPER(mo.vertriebsphase) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(mo.vertriebsphase) = 'QUALIFIKATION' THEN 'Qualification'
        WHEN UPPER(mo.vertriebsphase) = 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN UPPER(mo.vertriebsphase) = 'WERTANGEBOT' THEN 'Value Proposition'
        WHEN UPPER(mo.vertriebsphase) = 'ENTSCHEIDUNGSTRÄGER IDENTIFIZIERT' THEN 'Id. Decision Makers'
        WHEN UPPER(mo.vertriebsphase) = 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN UPPER(mo.vertriebsphase) = 'ANGEBOT/PREISANGEBOT' THEN 'Proposal/Price Quote'
        WHEN UPPER(mo.vertriebsphase) = 'VERHANDLUNG/ÜBERPRÜFUNG' THEN 'Negotiation/Review'
        WHEN UPPER(mo.vertriebsphase) = 'GEWONNEN' THEN 'Closed Won'
        WHEN UPPER(mo.vertriebsphase) = 'VERLOREN' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN mo.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN mo.zieldatum
        WHEN mo.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mo.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN mo.auftragswert ~ '^[0-9]+\.[0-9]{3},[0-9]{2}$' THEN 
            CAST(REPLACE(REPLACE(mo.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN mo.auftragswert ~ '^[0-9]+,[0-9]{2}$' THEN 
            CAST(REPLACE(mo.auftragswert, ',', '.') AS DOUBLE PRECISION)
        WHEN mo.auftragswert ~ '^[0-9]+\.[0-9]{2}$' THEN 
            CAST(mo.auftragswert AS DOUBLE PRECISION)
        WHEN mo.auftragswert ~ '^[0-9]+$' THEN 
            CAST(mo.auftragswert AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    mo.waehrungscode AS "CurrencyIsoCode",
    mk.kundennummer AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk ON mo.kunden_ref = mk.kundennummer