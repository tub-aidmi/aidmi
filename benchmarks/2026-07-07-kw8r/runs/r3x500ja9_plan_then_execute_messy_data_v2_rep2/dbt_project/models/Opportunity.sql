{{ config(materialized='table') }}

WITH opportunity_raw AS (
    SELECT *
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
),
normalized_opportunity AS (
    SELECT
        -- Id: TRIM source id — already in consistent PREFIX-NNNNN format
        TRIM(o.id) AS "Id",

        -- Name: INITCAP and trim for consistency
        INITCAP(TRIM(o.name)) AS "Name",

        -- StageName: map all observed variants to target enum domain
        CASE UPPER(TRIM(o.stagename))
            WHEN 'PROSPECTING'   THEN 'Prospecting'
            WHEN 'PROSPECT'      THEN 'Prospecting'
            WHEN 'IN KONTAKT'    THEN 'Prospecting'
            WHEN 'QUALIFICATION' THEN 'Qualification'
            WHEN 'QUALIFIKATION' THEN 'Qualification'
            WHEN 'QUALI'         THEN 'Qualification'
            WHEN 'NEEDS ANALYSIS' THEN 'Needs Analysis'
            WHEN 'IN PRÜFUNG'    THEN 'Needs Analysis'
            WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
            WHEN 'WERTEPROPOSITION'  THEN 'Value Proposition'
            WHEN 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
            WHEN 'ID. ENTSCHEIDER'     THEN 'Id. Decision Makers'
            WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
            WHEN 'WIRKUNGSANALYSE'     THEN 'Perception Analysis'
            WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
            WHEN 'ANGEBOT/PREISANGABE'  THEN 'Proposal/Price Quote'
            WHEN 'NEGOTIATION/REVIEW'   THEN 'Negotiation/Review'
            WHEN 'VERHANDLUNG/ÜBERPRÜFUNG' THEN 'Negotiation/Review'
            WHEN 'WON'               THEN 'Closed Won'
            WHEN 'CLOSED WON'        THEN 'Closed Won'
            WHEN 'ABSCHLIESSANG (GEWONNEN)' THEN 'Closed Won'
            WHEN 'GESCHLOSSEN GEWONNEN'     THEN 'Closed Won'
            WHEN 'GESCHLOSSEN (GEWONNEN)'   THEN 'Closed Won'
            WHEN 'GEWONNEN'               THEN 'Closed Won'
            WHEN 'ABGESCHLOSSEN (GEWONNEN)' THEN 'Closed Won'
            WHEN 'CLOSED LOST'     THEN 'Closed Lost'
            WHEN 'LOST'            THEN 'Closed Lost'
            WHEN 'VERLOREN'        THEN 'Closed Lost'
            WHEN 'ABSCHLIESANG (VERLOREN)'  THEN 'Closed Lost'
            WHEN 'GESCHLOSSEN VERLOREN'     THEN 'Closed Lost'
            WHEN 'GESCHLOSSEN (VERLOREN)'   THEN 'Closed Lost'
            WHEN 'ABGESCHLOSSEN (VERLOREN)' THEN 'Closed Lost'
            ELSE NULL
        END AS "StageName",

        -- CloseDate: multi-format parser → ISO YYYY-MM-DD text; NULL on unparseable/missing
        CASE
            WHEN TRIM(o.closedate) IS NULL OR TRIM(o.closedate) = '' THEN NULL
            WHEN TRIM(o.closedate) ~ '^\d{4}-\d{2}-\d{2}$'  THEN TO_DATE(TRIM(o.closedate), 'YYYY-MM-DD')::TEXT
            WHEN TRIM(o.closedate) ~ '^\d{8}$'               THEN
                SUBSTR(TRIM(o.closedate), 1, 4) || '-' ||
                SUBSTR(TRIM(o.closedate), 5, 2) || '-' ||
                SUBSTR(TRIM(o.closedate), 7, 2)
            WHEN TRIM(o.closedate) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN
                TO_DATE(TRIM(o.closedate), 'DD.MM.YYYY')::TEXT
            WHEN TRIM(o.closedate) ~ '^\d{1,2}/\d{1,2}/\d{4}$'  THEN
                TO_DATE(TRIM(o.closedate), 'MM/DD/YYYY')::TEXT
            ELSE NULL
        END AS "CloseDate",

        -- Amount: strip currency prefix/symbols, handle European (dot+comma) vs US formats; NULL for unparseable or literal "None"
        CASE
            WHEN TRIM(o.amount) IS NULL OR TRIM(o.amount) = '' THEN NULL
            WHEN UPPER(TRIM(o.amount)) = 'NONE'               THEN NULL
            ELSE CAST(
                CASE
                    -- European format: dots as thousands, comma as decimal  (e.g. "377.160,56" or "EUR 377.160,56")
                    WHEN REGEXP_REPLACE(TRIM(o.amount), '^[A-Z]+[ ]*', '', 'i') ~ '\.' AND
                         REGEXP_REPLACE(TRIM(o.amount), '^[A-Z]+[ ]*', '', 'i') ~ ',' AND
                         POSITION(',' IN REGEXP_REPLACE(TRIM(o.amount), '^[A-Z]+[ ]*', '', 'i')) >
                         POSITION('.' IN REGEXP_REPLACE(TRIM(o.amount), '^[A-Z]+[ ]*', '', 'i'))
                    THEN CAST(
                        REPLACE(
                            REPLACE(REGEXP_REPLACE(TRIM(o.amount), '^[A-Z]+[ ]*', '', 'i'), '.', ''),
                            ',', '.'
                        ) AS DOUBLE PRECISION)

                    -- US / plain decimal format (e.g. "-383632.13" or "EUR 42543.61")
                    ELSE CAST(
                        REGEXP_REPLACE(REGEXP_REPLACE(TRIM(o.amount), '^[A-Z]+[ ]*', '', 'i'), '[€$£]', '', 'g')
                    AS DOUBLE PRECISION)
                END
            AS DOUBLE PRECISION)
        END AS "Amount",

        -- CurrencyIsoCode: normalize to standard 3-letter codes
        CASE UPPER(TRIM(o.currencyisocode))
            WHEN 'EUR'      THEN 'EUR'
            WHEN 'EURO'     THEN 'EUR'
            WHEN '€'        THEN 'EUR'
            WHEN 'USD'      THEN 'USD'
            WHEN '$'        THEN 'USD'
            WHEN 'DOLLAR'   THEN 'USD'
            WHEN 'GBP'      THEN 'GBP'
            WHEN '£'        THEN 'GBP'
            WHEN 'CHF'      THEN 'CHF'
            ELSE UPPER(TRIM(o.currencyisocode))
        END AS "CurrencyIsoCode",

        -- AccountId: source accountid already matches account.id format — just TRIM
        TRIM(o.accountid) AS "AccountId",

        -- Legacy_Opportunity_ID__c: direct copy of source id for row-level verification
        TRIM(o.id) AS "Legacy_Opportunity_ID__c",

        -- Audit fields with deterministic defaults
        CURRENT_DATE::TEXT AS "CreatedDate",
        CURRENT_DATE::TEXT AS "LastModifiedDate",
        0 AS "IsDeleted"

    FROM opportunity_raw o
)

SELECT * FROM normalized_opportunity