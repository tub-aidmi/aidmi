{{ config(materialized='table') }}

SELECT
    CASE
        WHEN TRIM(chance_id) ~ '^O[0-9]+$' THEN '006' || SUBSTRING(LOWER(TRIM(chance_id)), 2)
        ELSE '006' || LEFT(MD5(TRIM(chance_id)), 15)
    END AS "Id",

    COALESCE(INITCAP(TRIM(bezeichnung)), 'Unknown') AS "Name",

    CASE
        WHEN LOWER(TRIM(phase)) IN ('akquise', 'neukundengewinnung', 'first contact') THEN 'Prospecting'
        WHEN LOWER(TRIM(phase)) IN ('qualifikation', 'qualification', 'qualifizierung') THEN 'Qualification'
        WHEN LOWER(TRIM(phase)) IN ('bedarfsanalyse', 'needs analysis', 'bedarfsermittlung') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(phase)) IN ('wert proposition', 'value proposition', 'nutzen aufzeigen', 'konzepterstellung') THEN 'Value Proposition'
        WHEN LOWER(TRIM(phase)) IN ('entscheidungsträger identifizieren', 'id. decision makers', 'multipler identification', 'key player analyse') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(phase)) IN ('wahrnehmungsanalyse', 'perception analysis', 'analysepipeline', 'vergleich') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(phase)) IN ('angebot', 'preisangebot', 'proposal', 'price quote', 'angebotserstellung', 'vorschlag') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(phase)) IN ('verhandlung', 'negotiation', 'klausur', 'review', 'verhandlungsphase', 'konditionenbesprechung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(phase)) IN ('abschluss erfolgreich', 'gewonnen', 'closed won', 'verkauf erfolgreich', 'successful closing') THEN 'Closed Won'
        WHEN LOWER(TRIM(phase)) IN ('geschlossen verloren', 'verloren', 'closed lost', 'abgelehnt', 'failed closing', 'nicht gewonnen') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",

    CASE
        WHEN TRIM(abschlussdatum) IS NULL OR TRIM(abschlussdatum) = '' THEN NULL
        WHEN TRIM(abschlussdatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(abschlussdatum), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(abschlussdatum) ~ '^\d{8}$' THEN SUBSTR(TRIM(abschlussdatum), 1, 4) || '-' || SUBSTR(TRIM(abschlussdatum), 5, 2) || '-' || SUBSTR(TRIM(abschlussdatum), 7, 2)
        ELSE NULL
    END AS "CloseDate",

    CAST(volumen AS DOUBLE PRECISION) AS "Amount",

    CASE
        WHEN LOWER(TRIM(waehrung)) IN ('eur', 'euro', '€', 'euros') THEN 'EUR'
        WHEN LOWER(TRIM(waehrung)) IN ('usd', 'us dollar', 'us dollars', '$', 'dollars') THEN 'USD'
        WHEN LOWER(TRIM(waehrung)) IN ('gbp', 'pound sterling', '£', 'pounds', 'british pound') THEN 'GBP'
        WHEN LOWER(TRIM(waehrung)) IN ('chf', 'swiss franc', 'swiss francs', 'francs') THEN 'CHF'
        WHEN LOWER(TRIM(waehrung)) IN ('cad', 'canadian dollar', 'canadian dollars') THEN 'CAD'
        WHEN TRIM(waehrung) ~ '^[A-Z]{3}$' THEN UPPER(TRIM(waehrung))
        ELSE NULL
    END AS "CurrencyIsoCode",

    CASE
        WHEN TRIM(kd_nr) ~ '^K\d+$' THEN '001' || SUBSTRING(TRIM(kd_nr), 2)
        ELSE '001' || LEFT(MD5(TRIM(kd_nr)), 15)
    END AS "AccountId",

    TRIM(chance_id) AS "Legacy_Opportunity_ID__c",

    CURRENT_DATE::TEXT AS "CreatedDate",

    CURRENT_DATE::TEXT AS "LastModifiedDate",

    CAST(0 AS INTEGER) AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}