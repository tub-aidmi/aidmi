{{ config(materialized='table') }}

SELECT
    opp_kennung AS "Id",
    COALESCE(TRIM(titel), 'N/A') AS "Name",
    CASE LOWER(TRIM(vertriebsphase))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'in kontakt' THEN 'Prospecting'
        WHEN 'prospect' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'quali' THEN 'Qualification'
        WHEN 'qualifikation' THEN 'Qualification'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'abgeschlossen (gewonnen)' THEN 'Closed Won'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        WHEN 'abgeschlossen (verloren)' THEN 'Closed Lost'
        WHEN 'verloren' THEN 'Closed Lost'
        WHEN 'lost' THEN 'Closed Lost'
        ELSE 'Qualification' -- Default for NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN zieldatum ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN TO_DATE(zieldatum, 'YYYY-MM-DD')
                WHEN zieldatum ~ '^\\d{2}\\.\\d{2}\\.\\d{4}$' THEN TO_DATE(zieldatum, 'DD.MM.YYYY')
                WHEN zieldatum ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN TO_DATE(zieldatum, 'MM/DD/YYYY')
                WHEN zieldatum ~ '^\\d{8}$' THEN TO_DATE(zieldatum, 'YYYYMMDD')
                ELSE NULL
            END,
            'YYYY-MM-DD'
        ),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "CloseDate",
    CASE
        WHEN auftragswert IS NULL OR TRIM(auftragswert) = '' THEN NULL
        ELSE CAST(REPLACE(REPLACE(TRIM(REGEXP_REPLACE(
            auftragswert,
            '[€$£]|(EUR|CHF|USD|GBP|Euro|Dollar)', '', 'gi'
        )), '.', ''), ',', '.') AS DOUBLE PRECISION)
    END AS "Amount",
    CASE LOWER(TRIM(waehrungscode))
        WHEN 'chf' THEN 'CHF'
        WHEN 'eur' THEN 'EUR'
        WHEN 'euro' THEN 'EUR'
        WHEN '€' THEN 'EUR'
        WHEN 'usd' THEN 'USD'
        WHEN 'dollar' THEN 'USD'
        WHEN '$' THEN 'USD'
        WHEN 'gbp' THEN 'GBP'
        WHEN '£' THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    REPLACE(kunden_ref, 'KD-', 'CUST-') AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source(source_name, source_table) }}
