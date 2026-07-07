-- noinspection SqlNoDataSourceInspection
-- noinspection SqlDialectInspection

{{ config(materialized='table') }}

WITH opportunities_cleaned AS (

    SELECT
        TRIM(mo.opp_kennung) AS opp_kennung,
        TRIM(mo.titel) AS titel,
        TRIM(mo.vertriebsphase) AS vertriebsphase,
        TRIM(mo.zieldatum) AS zieldatum,
        TRIM(mo.auftragswert) AS auftragswert,
        TRIM(mo.waehrungscode) AS waehrungscode,
        TRIM(mo.kunden_ref) AS kunden_ref,
        TRIM(mk.kundennummer) AS account_kundennummer
    FROM
        {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
    LEFT JOIN
        {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
        ON mo.kunden_ref = mk.kundennummer

)

SELECT
    opp_kennung AS "Id",
    COALESCE(titel, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(vertriebsphase) IN ('won', 'closed won') THEN 'Closed Won'
        WHEN LOWER(vertriebsphase) IN ('lost', 'verloren') THEN 'Closed Lost'
        WHEN LOWER(vertriebsphase) IN ('qualifikation', 'quali', 'qualification') THEN 'Qualification'
        WHEN LOWER(vertriebsphase) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(vertriebsphase) = 'in prüfung' THEN 'Perception Analysis'
        WHEN LOWER(vertriebsphase) = 'in kontakt' THEN 'Prospecting'
        ELSE 'Prospecting' -- Default for other/unknown active stages, as StageName is NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN zieldatum
            WHEN zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Fallback for unparseable or NULL dates, as CloseDate is NOT NULL
    ) AS "CloseDate",
    NULLIF(
        REPLACE(
            REPLACE(
                REGEXP_REPLACE(auftragswert, '[^0-9,.-]+', '', 'g'), -- Remove non-numeric except comma, dot, minus
                '.', '' -- Remove all dots (assuming European thousands separator)
            ),
            ',', '.' -- Replace comma with dot (assuming European decimal separator)
        ),
        ''
    )::DOUBLE PRECISION AS "Amount",
    CASE
        WHEN LOWER(waehrungscode) IN ('euro', 'eur', '€') THEN 'EUR'
        WHEN LOWER(waehrungscode) IN ('usd', 'dollar') THEN 'USD'
        WHEN LOWER(waehrungscode) IN ('chf') THEN 'CHF'
        WHEN LOWER(waehrungscode) IN ('gbp', '£') THEN 'GBP'
        ELSE UPPER(waehrungscode)
    END AS "CurrencyIsoCode",
    account_kundennummer AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    '2023-01-01T00:00:00Z' AS "CreatedDate",
    '2023-01-01T00:00:00Z' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    opportunities_cleaned