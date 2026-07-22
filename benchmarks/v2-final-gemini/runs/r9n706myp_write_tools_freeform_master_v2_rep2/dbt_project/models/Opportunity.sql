{{
  config(materialized='table')
}}

SELECT
    MD5(TRIM(opp_kennung)) AS "Id",
    COALESCE(TRIM(titel), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(vertriebsphase)) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
        WHEN LOWER(TRIM(vertriebsphase)) = 'in prüfung' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('won', 'gewonnen', 'closed won', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    COALESCE(
        CASE
            WHEN TRIM(zieldatum) ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN TO_DATE(TRIM(zieldatum), 'YYYY-MM-DD')
            WHEN TRIM(zieldatum) ~ '^\\d{2}\\.\\d{2}\\.\\d{4}$' THEN TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY')
            WHEN TRIM(zieldatum) ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN TO_DATE(TRIM(zieldatum), 'MM/DD/YYYY')
            WHEN TRIM(zieldatum) ~ '^\\d{8}$' THEN TO_DATE(TRIM(zieldatum), 'YYYYMMDD')
            ELSE NULL
        END,
        CURRENT_DATE
    )::TEXT AS "CloseDate",
    CASE
        WHEN TRIM(auftragswert) IS NULL OR TRIM(auftragswert) = '' THEN NULL
        ELSE
            (SELECT
                CASE
                    WHEN cleaned_amount = '' THEN NULL
                    WHEN cleaned_amount ~ '^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN cleaned_amount::DOUBLE PRECISION
                    ELSE NULL
                END
            FROM (
                SELECT
                    CASE
                        WHEN POSITION(',' IN cleaned_initial) > 0 AND POSITION('.' IN cleaned_initial) > 0 AND POSITION('.' IN cleaned_initial) < POSITION(',' IN cleaned_initial) THEN -- European format (e.g., 1.234,56)
                            REPLACE(REPLACE(cleaned_initial, '.', ''), ',', '.')
                        WHEN POSITION(',' IN cleaned_initial) > 0 AND POSITION('.' IN cleaned_initial) > 0 AND POSITION(',' IN cleaned_initial) < POSITION('.' IN cleaned_initial) THEN -- US format (e.g., 1,234.56)
                            REPLACE(cleaned_initial, ',', '')
                        WHEN POSITION(',' IN cleaned_initial) > 0 THEN -- Only comma, assume European decimal (e.g., 123,45)
                            REPLACE(cleaned_initial, ',', '.')
                        ELSE -- Only dot, or no separator, assume US decimal (e.g., 123.45)
                            cleaned_initial
                    END AS cleaned_amount
                FROM (
                    SELECT
                        REGEXP_REPLACE(TRIM(auftragswert), '[^0-9.,-]+', '', 'g') AS cleaned_initial
                ) AS _sub_initial
            ) AS _sub_amount)
    END AS "Amount",
    CASE
        WHEN LOWER(TRIM(waehrungscode)) IN ('eur', 'euro', '€') THEN 'EUR'
        WHEN LOWER(TRIM(waehrungscode)) IN ('usd', '$', 'dollar') THEN 'USD'
        WHEN LOWER(TRIM(waehrungscode)) IN ('chf', 'sfr') THEN 'CHF'
        WHEN LOWER(TRIM(waehrungscode)) IN ('gbp', '£') THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    MD5(TRIM(kunden_ref)) AS "AccountId",
    TRIM(opp_kennung) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }}
