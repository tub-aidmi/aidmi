{{ config(materialized='table') }}

SELECT
    mo.opp_kennung AS "Id",
    TRIM(mo.titel) AS "Name",
    CASE
        WHEN TRIM(mo.vertriebsphase) = 'Akquise' THEN 'Prospecting'
        WHEN TRIM(mo.vertriebsphase) = 'Qualifikation' THEN 'Qualification'
        WHEN TRIM(mo.vertriebsphase) = 'Bedarfsanalyse' THEN 'Needs Analysis'
        WHEN TRIM(mo.vertriebsphase) = 'Wertargumentation' THEN 'Value Proposition'
        WHEN TRIM(mo.vertriebsphase) = 'Entscheider identifizieren' THEN 'Id. Decision Makers'
        WHEN TRIM(mo.vertriebsphase) = 'Wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN TRIM(mo.vertriebsphase) = 'Angebot/Preis' THEN 'Proposal/Price Quote'
        WHEN TRIM(mo.vertriebsphase) = 'Verhandlung/Prüfung' THEN 'Negotiation/Review'
        WHEN TRIM(mo.vertriebsphase) = 'Gewonnen' THEN 'Closed Won'
        WHEN TRIM(mo.vertriebsphase) = 'Verloren' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN mo.zieldatum ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN mo.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mo.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN mo.auftragswert ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN 
            CAST(REPLACE(REPLACE(mo.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN mo.auftragswert ~ '^[0-9]+,[0-9]+$' THEN 
            CAST(REPLACE(mo.auftragswert, ',', '.') AS DOUBLE PRECISION)
        WHEN mo.auftragswert ~ '^[0-9]+\.[0-9]+$' THEN 
            CAST(mo.auftragswert AS DOUBLE PRECISION)
        WHEN mo.auftragswert ~ '^[0-9]+$' THEN 
            CAST(mo.auftragswert AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    UPPER(TRIM(mo.waehrungscode)) AS "CurrencyIsoCode",
    mk.kundennummer AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk 
    ON TRIM(mo.kunden_ref) = TRIM(mk.kundennummer)