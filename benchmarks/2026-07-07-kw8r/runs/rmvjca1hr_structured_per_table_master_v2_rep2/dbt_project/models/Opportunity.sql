{{ config(materialized='table') }}

SELECT
    opp_kennung AS "Id",
    COALESCE(NULLIF(TRIM(titel), ''), opp_kennung) AS "Name",
    CASE
        WHEN LOWER(TRIM(vertriebsphase)) IN ('in kontakt', 'prospect', 'prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('qualification', 'quali', 'qualifikation', 'in prüfung') THEN 'Qualification'
        WHEN LOWER(TRIM(vertriebsphase)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(vertriebsphase)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('id. decision makers', 'decision maker identification') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(vertriebsphase)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('proposal/price quote', 'proposal price quote', 'proposal', 'angebot') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('negotiation/review', 'negotiation review', 'verhandlung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('abgeschlossen (gewonnen)', 'gewonnen', 'closed won', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('abgeschlossen (verloren)', 'verloren', 'closed lost', 'lost') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN TRIM(zieldatum) IS NULL OR TRIM(zieldatum) = '' THEN NULL
        WHEN TRIM(zieldatum) ~ '^\d{4}-\d{1,2}-\d{1,2}$'
            THEN TO_CHAR(TO_DATE(TRIM(zieldatum), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(zieldatum) ~ '^\d{8}$'
            THEN TO_CHAR(TO_DATE(TRIM(zieldatum), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(zieldatum) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(zieldatum) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM(zieldatum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(auftragswert) IS NULL OR UPPER(TRIM(auftragswert)) = 'NONE' OR TRIM(auftragswert) = '' THEN NULL::DOUBLE PRECISION
        ELSE
            CAST(
                CASE
                    -- European format: contains comma as decimal separator; dots are thousands separators
                    WHEN REGEXP_REPLACE(TRIM(auftragswert), '[^\d.,\-]', '') ~ '.*,.*'
                        THEN REPLACE(REPLACE(TRIM(auftragswert), '.', ''), ',', '.')::DOUBLE PRECISION
                    -- US or plain numeric format: just strip non-numeric except dot and minus
                    ELSE
                        CAST(
                            REGEXP_REPLACE(TRIM(auftragswert), '[^\d.\-]', '') AS DOUBLE PRECISION
                        )
                END
            )
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM(waehrungscode)) IN ('CHF', 'CH') THEN 'CHF'
        WHEN UPPER(TRIM(waehrungscode)) IN ('EUR', 'EURO') OR TRIM(waehrungscode) = '€' THEN 'EUR'
        WHEN UPPER(TRIM(waehrungscode)) IN ('GBP', 'POUND') OR TRIM(waehrungscode) IN ('£', 'GBR') THEN 'GBP'
        WHEN UPPER(TRIM(waehrungscode)) IN ('USD', 'DOLLAR') OR TRIM(waehrungscode) IN ('$', 'US$') THEN 'USD'
        ELSE NULL
    END AS "CurrencyIsoCode",
    CASE
        WHEN TRIM(kunden_ref) ~ '^KD-' THEN 'CUST-' || SUBSTR(TRIM(kunden_ref), 4)
        ELSE TRIM(kunden_ref)
    END AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}