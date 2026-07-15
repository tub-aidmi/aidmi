{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(stagename)) IN ('prospecting', 'prospect', 'prospecting', 'prospekt', 'in kontakt', 'in contact') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualification', 'quali', 'qualifikation', 'qualifizierung', 'kvalifikasjon') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) IN ('needs analysis', 'bedarfsermittlung', 'analysee', 'bedarfsanalyse', 'analysis', 'analyse') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('value proposition', 'wertversprechen', 'valuedrengung', 'vorteilsdarstellung') THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) IN ('id. decision makers', 'identifizierung entscheidungsträger', 'identify decision makers', 'decision maker identification') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) IN ('perception analysis', 'wahrnehmungsanalyse', 'perzeptionsanalyse', 'perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'angebot/preisanfrage', 'proposal price quote', 'kostenvoranschlag', 'proposition/prix') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'verhandlung', 'negotiation', 'negociation', 'prüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) IN ('closed won', 'gewonnen', 'abgeschlossen (gewonnen)', 'won', 'abgeschlossen') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'verloren', 'abgeschlossen (verloren)', 'lost', 'los') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN closedate IS NULL OR TRIM(closedate) = '' OR TRIM(closedate) = 'N/A' THEN NULL
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(closedate, 'YYYY-MM-DD')::TEXT
        WHEN closedate ~ '^\d{8}$' THEN TO_DATE(closedate, 'YYYYMMDD')::TEXT
        WHEN closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')::TEXT
        WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CAST(
        CASE
            WHEN TRIM(amount) = '' OR TRIM(amount) IS NULL THEN NULL
            ELSE REGEXP_REPLACE(
                REGEXP_REPLACE(REGEXP_REPLACE(TRIM(amount), '[^\d.,\-]', '', 'g'),
                    '^(\d+)\.(\d{3}),', '\1\2.', 'g'),
                ',', '.')::DOUBLE PRECISION
        END AS DOUBLE PRECISION
    ) AS "Amount",
    CASE
        WHEN LOWER(TRIM(currencyisocode)) IN ('usd', 'us dollar', 'dollar', '$') THEN 'USD'
        WHEN LOWER(TRIM(currencyisocode)) IN ('eur', 'euro', '€') THEN 'EUR'
        WHEN LOWER(TRIM(currencyisocode)) IN ('gbp', 'british pound', '£') THEN 'GBP'
        WHEN LOWER(TRIM(currencyisocode)) IN ('chf', 'swiss franc') THEN 'CHF'
        ELSE UPPER(TRIM(COALESCE(currencyisocode, '')))
    END AS "CurrencyIsoCode",
    CAST(accountid AS TEXT) AS "AccountId",
    CAST(id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM fixture_messy_data_v2_src.opportunity
