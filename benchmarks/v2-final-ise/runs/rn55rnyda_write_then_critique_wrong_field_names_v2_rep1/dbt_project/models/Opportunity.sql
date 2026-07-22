{{ config(materialized='table') }}

SELECT
    c."chance_id" AS "Id",
    COALESCE(TRIM(c."bezeichnung"), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(c."phase")) IN ('prospecting', 'akquise', 'acquisition') THEN 'Prospecting'
        WHEN LOWER(TRIM(c."phase")) IN ('qualification', 'qualifikation', 'qualifying') THEN 'Qualification'
        WHEN LOWER(TRIM(c."phase")) IN ('needs analysis', 'bedarfsanalyse', 'need analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(c."phase")) IN ('value proposition', 'wertangebot', 'value prop') THEN 'Value Proposition'
        WHEN LOWER(TRIM(c."phase")) IN ('decision makers', 'entscheidungsträger', 'decision maker identification') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(c."phase")) IN ('perception analysis', 'wahrnehmungsanalyse', 'perception check') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(c."phase")) IN ('proposal', 'price quote', 'angebot', 'provisional quote', 'price quotation') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(c."phase")) IN ('negotiation', 'review', 'verhandlung', 'negotiations', 'reviews') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(c."phase")) IN ('closed won', 'gewonnen', 'won', 'deal closed', 'erfolgreich') THEN 'Closed Won'
        WHEN LOWER(TRIM(c."phase")) IN ('closed lost', 'verloren', 'lost', 'deal lost', 'nicht erfolgreich') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN c."abschlussdatum" IS NOT NULL AND TRIM(c."abschlussdatum") != '' THEN
            CASE
                WHEN c."abschlussdatum" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(c."abschlussdatum"), 'DD.MM.YYYY')::TEXT
                WHEN c."abschlussdatum" ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(c."abschlussdatum")
                WHEN c."abschlussdatum" ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(c."abschlussdatum"), 'MM/DD/YYYY')::TEXT
                ELSE NULL
            END
        ELSE NULL
    END AS "CloseDate",
    c."volumen" AS "Amount",
    TRIM(c."waehrung") AS "CurrencyIsoCode",
    '001' || LPAD(TRIM(c."kd_nr"), 12, '0') AS "AccountId",
    c."chance_id" AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c