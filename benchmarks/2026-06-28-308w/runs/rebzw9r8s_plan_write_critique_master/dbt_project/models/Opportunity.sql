
{{ config(materialized='table') }}

WITH raw_opportunities AS (
    SELECT
        TRIM(opp_kennung) AS opp_kennung,
        TRIM(titel) AS titel,
        TRIM(vertriebsphase) AS vertriebsphase,
        TRIM(zieldatum) AS zieldatum,
        TRIM(auftragswert) AS auftragswert,
        TRIM(waehrungscode) AS waehrungscode,
        TRIM(kunden_ref) AS kunden_ref
    FROM {{ source('fixture_master_src', 'master_opportunities') }}
),
cleaned_amounts AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        -- Step 1: Remove currency symbols and non-numeric characters (except for '.', ',', '-', digits)
        REGEXP_REPLACE(
            REGEXP_REPLACE(UPPER(auftragswert), '^(EUR|USD|GBP|CHF|€|\$)\s*', '', 'gi'),
            '[^0-9\.,-]', '', 'g'
        ) AS cleaned_amount_str,
        waehrungscode,
        kunden_ref
    FROM raw_opportunities
),
parsed_amounts_and_dates AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        -- Process zieldatum: parse and format as YYYY-MM-DD, default to current date if unparseable
        CAST(COALESCE(
            CASE
                WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(zieldatum, 'YYYY-MM-DD')
                WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(zieldatum, 'DD.MM.YYYY')
                ELSE NULL
            END,
            CURRENT_DATE
        ) AS TEXT) AS "CloseDate",

        -- Process auftragswert: clean, parse for European/US format, and cast to DOUBLE PRECISION
        COALESCE(
            CAST(
                CASE
                    WHEN cleaned_amount_str IS NULL OR cleaned_amount_str = '' THEN NULL
                    -- European format (comma as decimal, period as thousands): 1.234,56
                    WHEN cleaned_amount_str ~ '^-?\d+(\.\d{3})*,\d+$' THEN
                        REPLACE(REPLACE(cleaned_amount_str, '.', ''), ',', '.')
                    -- US format (period as decimal, comma as thousands): 1,234.56
                    WHEN cleaned_amount_str ~ '^-?\d+(,\d{3})*\.\d+$' THEN
                        REPLACE(cleaned_amount_str, ',', '')
                    -- Simple integer or float without thousands separators: 12345 or 123.45
                    WHEN cleaned_amount_str ~ '^-?\d+(\.\d+)?$' THEN
                        cleaned_amount_str
                    ELSE
                        '0' -- Fallback for truly unparseable strings, ensuring it's always convertible to DOUBLE PRECISION
                END AS DOUBLE PRECISION
            ), 0.0
        ) AS "Amount",
        waehrungscode,
        kunden_ref
    FROM cleaned_amounts
),
source_customers AS (
    SELECT
        TRIM(kundennummer) AS kundennummer
    FROM {{ source('fixture_master_src', 'master_kunden') }}
)

SELECT
    pa.opp_kennung AS "Id",
    COALESCE(pa.titel, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN UPPER(COALESCE(pa.vertriebsphase, '')) IN ('GESCHLOSSEN GEWONNEN', 'WON', 'CLOSED WON', 'ABGESCHLOSSEN (GEWONNEN)', 'GEWONNEN') THEN 'Closed Won'
        WHEN UPPER(COALESCE(pa.vertriebsphase, '')) IN ('GESCHLOSSEN VERLOREN', 'LOST', 'VERLOREN', 'CLOSED LOST', 'ABGESCHLOSSEN (VERLOREN)') THEN 'Closed Lost'
        WHEN UPPER(COALESCE(pa.vertriebsphase, '')) IN ('QUALIFIZIERUNG', 'QUALI', 'QUALIFICATION') THEN 'Qualification'
        WHEN UPPER(COALESCE(pa.vertriebsphase, '')) IN ('BEDARFSANALYSE') THEN 'Needs Analysis'
        WHEN UPPER(COALESCE(pa.vertriebsphase, '')) IN ('WERTANGEBOT') THEN 'Value Proposition'
        WHEN UPPER(COALESCE(pa.vertriebsphase, '')) IN ('ID. ENTSCHEIDUNGSTRÄGER') THEN 'Id. Decision Makers'
        WHEN UPPER(COALESCE(pa.vertriebsphase, '')) IN ('WAHRNEHMUNGSANALYSE') THEN 'Perception Analysis'
        WHEN UPPER(COALESCE(pa.vertriebsphase, '')) IN ('ANGEBOT/PREISANGEBOT', 'IN PRÜFUNG') THEN 'Proposal/Price Quote'
        WHEN UPPER(COALESCE(pa.vertriebsphase, '')) IN ('VERHANDLUNG/ÜBERPRÜFUNG') THEN 'Negotiation/Review'
        WHEN UPPER(COALESCE(pa.vertriebsphase, '')) IN ('PROSPEKTIERUNG', 'PROSPECTING', 'PROSPECT', 'IN KONTAKT') THEN 'Prospecting'
        ELSE 'Prospecting' -- Default value as it is NOT NULL
    END AS "StageName",
    pa."CloseDate",
    pa."Amount",
    COALESCE(
        CASE UPPER(COALESCE(pa.waehrungscode, ''))
            WHEN 'EUR' THEN 'EUR'
            WHEN '€' THEN 'EUR'
            WHEN 'USD' THEN 'USD'
            WHEN 'DOLLAR' THEN 'USD'
            WHEN 'GBP' THEN 'GBP'
            WHEN 'CHF' THEN 'CHF'
            ELSE NULL
        END,
        'USD'
    ) AS "CurrencyIsoCode",
    sc.kundennummer AS "AccountId",
    pa.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM parsed_amounts_and_dates AS pa
LEFT JOIN source_customers AS sc
    ON pa.kunden_ref = sc.kundennummer;
