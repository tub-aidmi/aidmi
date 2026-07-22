{{ config(materialized='table') }}

SELECT
    TRIM(mo.opp_kennung) AS "Id",
    COALESCE(TRIM(mo.titel), 'Unnamed Opportunity') AS "Name",
    CASE LOWER(TRIM(mo.vertriebsphase))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN TRIM(mo.zieldatum) = '' OR mo.zieldatum IS NULL THEN NULL
         -- DD.MM.YYYY
        WHEN TRIM(mo.zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(mo.zieldatum), 'DD.MM.YYYY')::TEXT
         -- YYYYMMDD (8 digits with no separators)
        WHEN TRIM(mo.zieldatum) ~ '^\d{8}$' THEN
            SUBSTR(TRIM(mo.zieldatum), 1, 4) || '-' ||
            SUBSTR(TRIM(mo.zieldatum), 5, 2) || '-' ||
            SUBSTR(TRIM(mo.zieldatum), 7, 2)
         -- MM/DD/YYYY
        WHEN TRIM(mo.zieldatum) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(mo.zieldatum), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(mo.auftragswert) = '' OR mo.auftragswert IS NULL THEN NULL
         -- European: dots as thousands, comma as decimal (e.g. "1.234,56" or "EUR 1.234,56")
        WHEN REGEXP_REPLACE(TRIM(mo.auftragswert), '^(EUR|USD|GBP|CHF|\$\€\£\¥)\s*', '') ~ '^\d[\d\.]*,\d+$'
             AND REGEXP_REPLACE(TRIM(mo.auftragswert), '^(EUR|USD|GBP|CHF|\$\€\£\¥)\s*', '') ~ '\.'
            THEN CAST(
                REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(TRIM(mo.auftragswert), '^(EUR|USD|GBP|CHF|\$\€\£\¥)\s*', ''),
                        '\.', ''),
                    ',', '.')
                AS DOUBLE PRECISION)
         -- Plain number with comma as decimal only (e.g. "1234,56")
        WHEN REGEXP_REPLACE(TRIM(mo.auftragswert), '^(EUR|USD|GBP|CHF|\$\€\£\¥)\s*', '') ~ '^\d[\d]*,\d+$'
             AND NOT REGEXP_REPLACE(TRIM(mo.auftragswert), '^(EUR|USD|GBP|CHF|\$\€\£\¥)\s*', '') ~ '\.'
            THEN CAST(
                REPLACE(
                    REGEXP_REPLACE(TRIM(mo.auftragswert), '^(EUR|USD|GBP|CHF|\$\€\£\¥)\s*', ''), ',', '.')
                AS DOUBLE PRECISION)
         -- Plain decimal or integer — strip currency code/symbol, remove any remaining non-numeric chars except dots, then cast
        ELSE CAST(
            REGEXP_REPLACE(
                REGEXP_REPLACE(TRIM(mo.auftragswert), '^(EUR|USD|GBP|CHF|\$\€\£\¥)\s*', ''),
                 '[^\d.]', '')
            AS DOUBLE PRECISION)
    END AS "Amount",
    UPPER(TRIM(mo.waehrungscode)) AS "CurrencyIsoCode",
    mk.kundennummer AS "AccountId",
    TRIM(mo.opp_kennung) AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
     0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk
    ON TRIM(mk.kundennummer) = TRIM(mo.kunden_ref)