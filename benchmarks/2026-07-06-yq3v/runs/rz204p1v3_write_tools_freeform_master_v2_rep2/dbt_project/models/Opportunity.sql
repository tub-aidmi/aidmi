{{ config(materialized='table') }}

WITH cleaned_opportunities AS (
    SELECT
        TRIM(opp_kennung) AS opp_kennung,
        TRIM(titel) AS titel,
        TRIM(vertriebsphase) AS vertriebsphase,
        TRIM(zieldatum) AS zieldatum,
        TRIM(auftragswert) AS auftragswert,
        TRIM(waehrungscode) AS waehrungscode,
        TRIM(kunden_ref) AS kunden_ref
    FROM
        {{ source('fixture_master_v2_src', 'master_opportunities') }}
)
SELECT
    MD5(opp_kennung) AS "Id",
    COALESCE(titel, opp_kennung) AS "Name",
    CASE
        WHEN LOWER(vertriebsphase) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(vertriebsphase) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
        WHEN LOWER(vertriebsphase) IN ('closed won', 'won', 'abgeschlossen (gewonnen)', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(vertriebsphase) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(vertriebsphase) = 'in prüfung' THEN 'Negotiation/Review' -- Best guess
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN zieldatum
            WHEN zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE '1900-01-01' -- Fallback for NOT NULL
        END,
        '1900-01-01'
    ) AS "CloseDate",
    CASE
        WHEN auftragswert IS NULL OR auftragswert = 'None' THEN NULL
        ELSE CAST(
            CASE
                WHEN REGEXP_REPLACE(auftragswert, '[€$£]|(EUR|USD|CHF|GBP|Euro)\s*', '', 'gi') ~ '\d+\.\d+,\d+$' THEN -- European format (1.234,56)
                    REPLACE(REPLACE(REGEXP_REPLACE(auftragswert, '[€$£]|(EUR|USD|CHF|GBP|Euro)\s*', '', 'gi'), '.', ''), ',', '.')
                WHEN REGEXP_REPLACE(auftragswert, '[€$£]|(EUR|USD|CHF|GBP|Euro)\s*', '', 'gi') ~ '\d+,\d+$' THEN -- European format (123,45)
                    REPLACE(REGEXP_REPLACE(auftragswert, '[€$£]|(EUR|USD|CHF|GBP|Euro)\s*', '', 'gi'), ',', '.')
                ELSE -- US format or just numbers (1234.56, 123456)
                    REGEXP_REPLACE(auftragswert, '[€$£]|(EUR|USD|CHF|GBP|Euro)\s*', '', 'gi')
            END
        AS DOUBLE PRECISION)
    END AS "Amount",
    CASE
        WHEN LOWER(waehrungscode) IN ('usd', 'dollar', '$') THEN 'USD'
        WHEN LOWER(waehrungscode) IN ('gbp', '£') THEN 'GBP'
        WHEN LOWER(waehrungscode) IN ('eur', 'euro', '€') THEN 'EUR'
        WHEN LOWER(waehrungscode) = 'chf' THEN 'CHF'
        ELSE NULL
    END AS "CurrencyIsoCode",
    MD5(kunden_ref) AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_opportunities
