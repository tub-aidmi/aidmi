{{ config(materialized='table') }}

SELECT
    MD5(opp.opp_kennung) AS "Id",
    COALESCE(TRIM(opp.titel), MD5(opp.opp_kennung)) AS "Name",
    CASE
        WHEN opp.vertriebsphase ILIKE 'Anbahnung' THEN 'Prospecting'
        WHEN opp.vertriebsphase ILIKE 'Qualifizierung' THEN 'Qualification'
        WHEN opp.vertriebsphase ILIKE 'Bedarfsanalyse' THEN 'Needs Analysis'
        WHEN opp.vertriebsphase ILIKE 'Wertangebot' THEN 'Value Proposition'
        WHEN opp.vertriebsphase ILIKE 'Id. Entscheidungsträger' THEN 'Id. Decision Makers'
        WHEN opp.vertriebsphase ILIKE 'Wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN opp.vertriebsphase ILIKE 'Angebot/Preisangebot' THEN 'Proposal/Price Quote'
        WHEN opp.vertriebsphase ILIKE 'Verhandlung/Überprüfung' THEN 'Negotiation/Review'
        WHEN opp.vertriebsphase ILIKE 'Geschlossen gewonnen' THEN 'Closed Won'
        WHEN opp.vertriebsphase ILIKE 'Geschlossen verloren' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(opp.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(opp.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(opp.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Fallback for NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN opp.auftragswert ~ '^\s*[-+]?\d{1,3}(\.\d{3})*,\d+$' THEN -- European format 1.234,56
            REPLACE(REPLACE(opp.auftragswert, '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN opp.auftragswert ~ '^\s*[-+]?\d+(\.\d+)?$' THEN -- US format 1234.56
            opp.auftragswert::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    UPPER(TRIM(opp.waehrungscode)) AS "CurrencyIsoCode",
    MD5(opp.kunden_ref) AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opp