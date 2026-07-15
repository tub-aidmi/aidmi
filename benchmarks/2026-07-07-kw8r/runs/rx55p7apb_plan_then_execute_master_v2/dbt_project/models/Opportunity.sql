{{ config(materialized='table') }}
WITH opportunity_data AS (
    SELECT
        mo.opp_kennung,
        mo.titel,
        mo.vertriebsphase,
        mo.zieldatum,
        mo.auftragswert,
        mo.waehrungscode,
        mo.kunden_ref,
        mk.kundennummer AS account_id
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk
        ON TRIM(mo.kunden_ref) = TRIM(mk.kundennummer)
)
SELECT
    opp_kennung AS "Id",
    COALESCE(NULLIF(INITCAP(TRIM(titel)), ''), 'Untitled Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(vertriebsphase)) = 'PROSPEKTIERUNG' THEN 'Prospecting'
        WHEN UPPER(TRIM(vertriebsphase)) = 'QUALIFIKATION' THEN 'Qualification'
        WHEN UPPER(TRIM(vertriebsphase)) = 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(vertriebsphase)) = 'WERTVORSCHLAG' THEN 'Value Proposition'
        WHEN UPPER(TRIM(vertriebsphase)) = 'ENTSCHEIDUNGSTRÄGER IDENTIFIZIEREN' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(vertriebsphase)) = 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(vertriebsphase)) = 'ANGEBOT' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(vertriebsphase)) = 'VERHANDLUNG' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(vertriebsphase)) = 'GESCHLOSSEN GEWONNEN' THEN 'Closed Won'
        WHEN UPPER(TRIM(vertriebsphase)) = 'GESCHLOSSEN VERLOREN' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN auftragswert ~ '^[\d\s.,-]+$' THEN
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(REGEXP_REPLACE(auftragswert, '[^\d.,-]', '', 'g'), '\.', '', 'g'),
                    ',',
                    '.',
                    'g'
                )
                AS DOUBLE PRECISION
            )
        ELSE NULL
    END AS "Amount",
    UPPER(TRIM(waehrungscode)) AS "CurrencyIsoCode",
    account_id AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    '2023-01-01' AS "CreatedDate",
    '2023-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunity_data