{{ config(materialized='table') }}

WITH opp AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
),

stage_map AS (
    SELECT
        opp_kennung AS "Id",
        INITCAP(TRIM(titel)) AS "Name",

        -- Stage mapping from various German/English phrasings
        CASE
            WHEN LOWER(TRIM(vertriebsphase)) IN ('prospect', 'prospecting', 'in kontakt') THEN 'Prospecting'
            WHEN LOWER(TRIM(vertriebsphase)) IN ('quali', 'qualifikation', 'qualification') THEN 'Qualification'
            WHEN LOWER(TRIM(vertriebsphase)) IN ('needs analysis') THEN 'Needs Analysis'
            WHEN LOWER(TRIM(vertriebsphase)) IN ('value proposition', 'value prop') THEN 'Value Proposition'
            WHEN LOWER(TRIM(vertriebsphase)) IN ('id. decision makers', 'identification of decision makers') THEN 'Id. Decision Makers'
            WHEN LOWER(TRIM(vertriebsphase)) IN ('perception analysis') THEN 'Perception Analysis'
            WHEN LOWER(TRIM(vertriebsphase)) IN ('proposal/price quote', 'angebot/preisanfrage') THEN 'Proposal/Price Quote'
            WHEN LOWER(TRIM(vertriebsphase)) IN ('negotiation/review', 'verhandlung', 'negotiation') THEN 'Negotiation/Review'
            WHEN LOWER(TRIM(vertriebsphase)) IN ('closed won', 'gewonnen', 'abgeschlossen (gewonnen)', 'won') THEN 'Closed Won'
            WHEN LOWER(TRIM(vertriebsphase)) IN ('closed lost', 'verloren', 'abgeschlossen (verloren)', 'lost') THEN 'Closed Lost'
            ELSE NULL
        END AS "StageName",

        -- Date parsing: DD.MM.YYYY and YYYYMMDD are the observed formats
        CASE
            WHEN TRIM(zieldatum) IS NULL OR TRIM(zieldatum) = '' THEN NULL
            WHEN zieldatum ~ '^\d{8}$' 
                THEN TO_CHAR(TO_DATE(TRIM(zieldatum), 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' 
                THEN TO_CHAR(TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' 
                THEN TRIM(zieldatum)
            ELSE NULL
        END AS "CloseDate",

        -- Amount: handle BOTH European (comma=decimal, dot=thousands) and standard (dot=decimal) formats
        CASE
            WHEN TRIM(auftragswert) IS NULL OR TRIM(UPPER(auftragswert)) IN ('NONE', '') THEN NULL
             -- If comma present → European format: strip text/dots, swap comma→dot
            WHEN auftragswert ~ '[,]' THEN
                CAST(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(TRIM(auftragswert), '[A-Za-z\s€$£]', '', 'g'),
                             '\.', ''),           -- remove thousand-sep dots
                     ',', '.')                         -- decimal comma → dot
                AS DOUBLE PRECISION)
             -- No comma → standard format: keep only digits, minus sign, dot
            ELSE
                CAST(
                    REGEXP_REPLACE(TRIM(auftragswert), '[^\d.\-]', '', 'g') 
                AS DOUBLE PRECISION)
        END AS "Amount",

        -- Currency: normalize all observed variants to ISO 4217 codes
        CASE
            WHEN LOWER(TRIM(waehrungscode)) IN ('chf', 'sfr', 'schweizer Franken') THEN 'CHF'
            WHEN LOWER(TRIM(waehrungscode)) IN ('eur', 'euro', '€')                          THEN 'EUR'
            WHEN LOWER(TRIM(waehrungscode)) IN ('usd', 'dollar', '$', 'dollars')              THEN 'USD'
            WHEN LOWER(TRIM(waehrungscode)) IN ('gbp', '£', 'pound', 'pounds')                THEN 'GBP'
            ELSE UPPER(TRIM(REGEXP_REPLACE(waehrungscode, '[^A-Za-z]', '')))
        END AS "CurrencyIsoCode",

        kunden_ref,
        TRIM(opp_kennung) AS "Legacy_Opportunity_ID__c"
    FROM opp
),

account_key_map AS (
    -- Transform KD- prefix on opp side to match CUST- prefix in master_kunden
    SELECT
        t."Id",
        t."Name",
        COALESCE(t."StageName", 'Prospecting') AS "StageName",
        COALESCE(t."CloseDate", NULL) AS "CloseDate",
        t."Amount",
        t."CurrencyIsoCode",
        cust.kundennummer AS "AccountId",
        t."Legacy_Opportunity_ID__c",
        NULL::TEXT AS "CreatedDate",
        NULL::TEXT AS "LastModifiedDate",
        0 AS "IsDeleted"
    FROM stage_map t
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} cust
        ON REPLACE(t.kunden_ref, 'KD-', 'CUST-') = cust.kundennummer

)

SELECT * FROM account_key_map