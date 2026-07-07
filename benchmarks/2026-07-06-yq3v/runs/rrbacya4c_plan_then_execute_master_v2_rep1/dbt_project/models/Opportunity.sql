{{ config(materialized='table') }}

WITH
opportunities_source AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
)
SELECT
    gen_random_uuid()::text AS "Id",
    COALESCE(TRIM(opportunities_source.titel), 'Unknown Opportunity') AS "Name",
    CASE LOWER(TRIM(opportunities_source.vertriebsphase))
        WHEN 'anbahnung' THEN 'Prospecting'
        WHEN 'qualifizierung' THEN 'Qualification'
        WHEN 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN 'wertangebot' THEN 'Value Proposition'
        WHEN 'entscheidungsträger' THEN 'Id. Decision Makers'
        WHEN 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN 'angebot' THEN 'Proposal/Price Quote'
        WHEN 'verhandlung' THEN 'Negotiation/Review'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'verloren' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default value as StageName is NOT NULL
    END AS "StageName",
    COALESCE(
        (CASE
            WHEN opportunities_source.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(opportunities_source.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            ELSE NULL
        END),
        TO_CHAR(NOW(), 'YYYY-MM-DD') -- Fallback for NOT NULL CloseDate
    ) AS "CloseDate",
    NULLIF(
        TRIM(
            CASE
                WHEN opportunities_source.auftragswert IS NULL THEN NULL
                WHEN REGEXP_REPLACE(opportunities_source.auftragswert, '[^0-9.,]', '', 'g') ~ '^[0-9]+\.[0-9]{3}(,[0-9]{1,2})?$' -- e.g., 1.234,56 (European thousands dot, decimal comma)
                    THEN REPLACE(REPLACE(REGEXP_REPLACE(opportunities_source.auftragswert, '[^0-9.,]', '', 'g'), '.', ''), ',', '.')
                WHEN REGEXP_REPLACE(opportunities_source.auftragswert, '[^0-9.,]', '', 'g') ~ '^[0-9]+,[0-9]{1,2}$' -- e.g., 1234,56 (European decimal comma, no thousands sep)
                    THEN REPLACE(REGEXP_REPLACE(opportunities_source.auftragswert, '[^0-9.,]', '', 'g'), ',', '.')
                WHEN REGEXP_REPLACE(opportunities_source.auftragswert, '[^0-9.,]', '', 'g') ~ '^[0-9]+,[0-9]{3}(\.[0-9]{1,2})?$' -- e.g., 1,234.56 (US thousands comma, decimal dot)
                    THEN REPLACE(REGEXP_REPLACE(opportunities_source.auftragswert, '[^0-9.,]', '', 'g'), ',', '')
                ELSE -- Assume standard US format (dot decimal) or no specific separators, remove commas if they exist.
                    REGEXP_REPLACE(REGEXP_REPLACE(opportunities_source.auftragswert, '[^0-9.]', '', 'g'), ',', '')
            END
        ),
        ''
    )::DOUBLE PRECISION AS "Amount",
    UPPER(TRIM(opportunities_source.waehrungscode)) AS "CurrencyIsoCode",
    MD5(opportunities_source.kunden_ref)::text AS "AccountId",
    TRIM(opportunities_source.opp_kennung) AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS') || 'Z' AS "CreatedDate",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS') || 'Z' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    opportunities_source
