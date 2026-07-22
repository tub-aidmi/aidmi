{{ config(materialized='table') }}

SELECT
    -- Generate deterministic Salesforce-style Opportunity Id (prefix 006 + 15 chars from md5 hash)
    CONCAT(
        '006',
        SUBSTRING(md5(CONCAT('OPP-', UPPER(TRIM(chance_id)))), 1, 15)
    ) AS "Id",

    INITCAP(TRIM(bezeichnung)) AS "Name",

    -- Map StageName from source phase values to target enum domain
    CASE
        WHEN LOWER(TRIM(phase)) IN ('prospecting', 'prospektierung', 'akquise') THEN 'Prospecting'
        WHEN LOWER(TRIM(phase)) IN ('qualification', 'qualifikation', 'qualifizierung') THEN 'Qualification'
        WHEN LOWER(TRIM(phase)) IN ('needs analysis', 'bedarfsanalyse', 'bedarfsermittlung') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(phase)) IN ('value proposition', 'wertproposition', 'nutzenbewertung') THEN 'Value Proposition'
        WHEN LOWER(TRIM(phase)) IN ('id. decision makers', 'entscheider identifizieren', 'identification of decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(phase)) IN ('perception analysis', 'wahrnehmungsanalyse', 'bildaustausch') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(phase)) IN ('proposal/price quote', 'angebot/preisangebot', 'vorschlag/preisanfrage', 'provisional price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(phase)) IN ('negotiation/review', 'verhandlung/prüfung', 'verhandlung', 'review') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(phase)) IN ('closed won', 'gewonnen', 'erfolgreich abgeschlossen') THEN 'Closed Won'
        WHEN LOWER(TRIM(phase)) IN ('closed lost', 'verloren', 'nicht erfolgreich') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",

    -- Parse CloseDate: handle YYYY-MM-DD first (ISO format), then DD.MM.YYYY, then YYYYMMDD
    CASE
        -- ISO 8601 date with hyphens: e.g. '2024-10-22'
        WHEN TRIM(abschlussdatum) IS NOT NULL AND TRIM(abschlussdatum) ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TO_CHAR(TO_DATE(TRIM(abschlussdatum), 'YYYY-MM-DD'), 'YYYY-MM-DD')

        -- DD.MM.YYYY format: e.g. '22.10.2024' — must start with exactly 2 digits then dot
        WHEN TRIM(abschlussdatum) IS NOT NULL AND TRIM(abschlussdatum) ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM(abschlussdatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')

        -- YYYYMMDD format (8 consecutive digits): e.g. '20241022'
        WHEN TRIM(abschlussdatum) IS NOT NULL AND TRIM(abschlussdatum) ~ '^\d{8}$'
            THEN TO_CHAR(TO_DATE(TRIM(abschlussdatum), 'YYYYMMDD'), 'YYYY-MM-DD')

        -- MM/DD/YYYY format: e.g. '10/22/2024'
        WHEN TRIM(abschlussdatum) IS NOT NULL AND TRIM(abschlussdatum) ~ '^\d{2}/\d{2}/\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM(abschlussdatum), 'MM/DD/YYYY'), 'YYYY-MM-DD')

        ELSE NULL
    END AS "CloseDate",

    -- Amount is already double precision in source; cast explicitly with proper syntax
    CAST(volumen AS DOUBLE PRECISION) AS "Amount",

    UPPER(TRIM(waehrung)) AS "CurrencyIsoCode",

    -- Generate AccountId deterministically (same formula as Account model uses for Id, referencing customer number kd_nr)
    CONCAT(
        '001',
        SUBSTRING(md5(CONCAT('ACCT-', UPPER(TRIM(kd_nr)))), 1, 15)
    ) AS "AccountId",

    -- Populate Legacy_Opportunity_ID__c from the source natural key chance_id
    TRIM(chance_id) AS "Legacy_Opportunity_ID__c",

    -- Timestamps
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "CreatedDate",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "LastModifiedDate",

    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}