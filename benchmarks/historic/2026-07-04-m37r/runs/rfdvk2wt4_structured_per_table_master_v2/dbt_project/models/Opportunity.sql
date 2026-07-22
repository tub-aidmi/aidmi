-- depends_on: {{ source('fixture_master_v2_src', 'master_opportunities') }}
-- depends_on: {{ source('fixture_master_v2_src', 'master_kunden') }}

{{ config(materialized='table') }}

SELECT
    MD5(opp.opp_kennung) AS "Id",
    COALESCE(TRIM(opp.titel), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('won', 'gewonnen', 'closed won', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('qualifikation', 'qualification', 'quali') THEN 'Qualification'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(opp.vertriebsphase)) = 'in prüfung' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for NOT NULL target
    END AS "StageName",
    COALESCE(
        (CASE WHEN opp.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD') ELSE NULL END),
        (CASE WHEN opp.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD') ELSE NULL END),
        (CASE WHEN opp.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD') ELSE NULL END),
        (CASE WHEN opp.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD') ELSE NULL END),
        CAST('2000-01-01' AS TEXT) -- Default for NOT NULL target and unparseable dates
    ) AS "CloseDate",
    CASE
        WHEN TRIM(opp.auftragswert) IS NULL THEN NULL
        WHEN TRIM(opp.auftragswert) ~ '^-?[0-9]{1,3}(\.[0-9]{3})*,[0-9]+$' THEN CAST(REPLACE(REPLACE(TRIM(opp.auftragswert), '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN TRIM(opp.auftragswert) ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN CAST(TRIM(opp.auftragswert) AS DOUBLE PRECISION)
        ELSE NULL -- Prefer NULL for unparseable amounts
    END AS "Amount",
    CASE
        WHEN LOWER(TRIM(opp.waehrungscode)) IN ('$', 'usd', 'dollar') THEN 'USD'
        WHEN LOWER(TRIM(opp.waehrungscode)) IN ('eur', '€', 'euro') THEN 'EUR'
        WHEN LOWER(TRIM(opp.waehrungscode)) IN ('£', 'gbp') THEN 'GBP'
        WHEN LOWER(TRIM(opp.waehrungscode)) IN ('chf') THEN 'CHF'
        ELSE NULL
    END AS "CurrencyIsoCode",
    MD5(k.kundennummer) AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opp
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
ON
    opp.kunden_ref = k.kundennummer