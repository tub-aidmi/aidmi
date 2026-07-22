{{ config(materialized='table') }}

SELECT
    MD5(opportunities.opp_kennung) AS "Id",
    opportunities.titel AS "Name",
    CASE
        WHEN LOWER(opportunities.vertriebsphase) IN ('won', 'closed won', 'abgeschlossen (gewonnen)', 'gewonnen', 'closedwon') THEN 'Closed Won'
        WHEN LOWER(opportunities.vertriebsphase) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(opportunities.vertriebsphase) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(opportunities.vertriebsphase) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
        WHEN LOWER(opportunities.vertriebsphase) = 'in prüfung' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    CASE
        WHEN opportunities.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opportunities.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN opportunities.zieldatum ~ '^\d{1,2}\/\d{1,2}\/\d{4}$' THEN TO_CHAR(TO_DATE(opportunities.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN opportunities.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(opportunities.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN opportunities.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opportunities.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Fallback for NOT NULL CloseDate
    END AS "CloseDate",
    CASE
        WHEN REPLACE(REPLACE(REPLACE(opportunities.auftragswert, 'EUR ', ''), '.', ''), ',', '.') ~ '^[+-]?(\\d+(\.\\d*)?|\\.\\d+)$'
        THEN CAST(REPLACE(REPLACE(REPLACE(opportunities.auftragswert, 'EUR ', ''), '.', ''), ',', '.') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    opportunities.waehrungscode AS "CurrencyIsoCode",
    MD5(kunden.kundennummer) AS "AccountId",
    opportunities.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opportunities
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
ON
    opportunities.kunden_ref = kunden.kundennummer
