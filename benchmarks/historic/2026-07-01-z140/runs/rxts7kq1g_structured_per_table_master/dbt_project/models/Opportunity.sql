{{ config(materialized='table') }}

SELECT
    opp.opp_kennung AS "Id",
    INITCAP(TRIM(opp.titel)) AS "Name",
    CASE
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('gewonnen', 'won', 'closed won', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('verloren', 'lost', 'closed lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('prospecting', 'prospect') THEN 'Prospecting'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('in prüfung', 'needs analysis', 'in kontakt') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('value proposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('id. decision makers', 'identifying decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('angebot/preisanfrage', 'proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('verhandlung', 'negotiation/review', 'negotiation') THEN 'Negotiation/Review'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN TRIM(opp.zieldatum) IS NULL OR TRIM(opp.zieldatum) = '' OR TRIM(opp.zieldatum) = 'N/A' OR TRIM(opp.zieldatum) = 'None' OR opp.zieldatum = '0000-00-00' THEN NULL
        WHEN opp.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(opp.zieldatum)
        WHEN opp.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(opp.zieldatum), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$' THEN TO_CHAR(TO_DATE(TRIM(opp.zieldatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN TO_CHAR(TO_DATE(TRIM(opp.zieldatum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN opp.auftragswert IS NULL OR TRIM(opp.auftragswert) = '' OR UPPER(TRIM(opp.auftragswert)) = 'NONE' THEN NULL
        ELSE CAST(
            CASE
                -- European format: contains both dot (thousands sep) and comma (decimal sep), e.g. "316.863,04"
                WHEN REGEXP_REPLACE(TRIM(opp.auftragswert), '[^\d,\-]', '', 'g') ~ '\.\d{3},\d' THEN
                    CAST(REPLACE(TRANSLATE(REGEXP_REPLACE(TRIM(opp.auftragswert), '[^\d,\-]', '', 'g'), '.', ''), ',', '.') AS DOUBLE PRECISION)
                -- European format with comma but no dot: e.g. "1.234,56" after stripping non-digits
                WHEN REGEXP_REPLACE(TRIM(opp.auftragswert), '[^\d,\-]', '', 'g') ~ ',\d$' AND LENGTH(REGEXP_REPLACE(TRIM(opp.auftragswert), '[^\d,\-]', '', 'g')) > 4 THEN
                    CAST(REPLACE(REGEXP_REPLACE(TRIM(opp.auftragswert), '[^\d,\-]', '', 'g'), ',', '.') AS DOUBLE PRECISION)
                -- Already clean US format or plain integer, e.g. "253569.24" or "-440691.0"
                ELSE CAST(REGEXP_REPLACE(TRIM(opp.auftragswert), '[^\d.\-]', '', 'g') AS DOUBLE PRECISION)
            END
        AS DOUBLE PRECISION)
    END AS "Amount",
    CASE
        WHEN LOWER(TRIM(opp.waehrungscode)) IN ('eur', '€') THEN 'EUR'
        WHEN LOWER(TRIM(opp.waehrungscode)) IN ('usd', 'dollar', '$') THEN 'USD'
        WHEN LOWER(TRIM(opp.waehrungscode)) IN ('chf') THEN 'CHF'
        WHEN LOWER(TRIM(opp.waehrungscode)) IN ('gbp', '£') THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    REGEXP_REPLACE(opp.kunden_ref, '^KD-', 'CUST-') AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_opportunities') }} opp