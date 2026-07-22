{{ config(materialized='table') }}

SELECT
    gen_random_uuid()::TEXT AS "Id",
    COALESCE(TRIM(o.titel), 'Unnamed Opportunity') AS "Name",
    CASE UPPER(TRIM(o.vertriebsphase))
        WHEN 'PROSPECTING' THEN 'Prospecting'
        WHEN 'QUALIFIKATION' THEN 'Qualification'
        WHEN 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN 'WERTVORSCHLAG' THEN 'Value Proposition'
        WHEN 'ENT. ENTSCHEIDER' THEN 'Id. Decision Makers'
        WHEN 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN 'ANGEBOT/PREISANGEBOT' THEN 'Proposal/Price Quote'
        WHEN 'VERHANDLUNG/ÜBERPRÜFUNG' THEN 'Negotiation/Review'
        WHEN 'GESCHLOSSEN GEWONNEN' THEN 'Closed Won'
        WHEN 'GESCHLOSSEN VERLOREN' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for StageName, as it's NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN o.zieldatum IS NULL OR TRIM(o.zieldatum) = '' THEN NULL
                WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(o.zieldatum, 'YYYY-MM-DD')
                WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(o.zieldatum, 'DD.MM.YYYY')
                WHEN o.zieldatum ~ '^\d{8}$' THEN TO_DATE(o.zieldatum, 'YYYYMMDD')
                WHEN o.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(o.zieldatum, 'MM/DD/YYYY')
                ELSE NULL
            END,
            'YYYY-MM-DD'
        ),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL CloseDate
    ) AS "CloseDate",
    CASE
        WHEN TRIM(REPLACE(REPLACE(TRIM(regexp_replace(o.auftragswert, '[^0-9,]+', '', 'g')), '.', ''), ',', '.')) = ''
        THEN NULL
        ELSE CAST(
            REPLACE(
                REPLACE(
                    TRIM(regexp_replace(o.auftragswert, '[^0-9,]+', '', 'g')),
                    '.',
                    ''
                ),
                ',',
                '.'
            ) AS DOUBLE PRECISION
        )
    END AS "Amount",
    TRIM(UPPER(o.waehrungscode)) AS "CurrencyIsoCode",
    a."Id" AS "AccountId",
    TRIM(o.opp_kennung) AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o
LEFT JOIN
    {{ ref('Account') }} AS a
ON
    TRIM(o.kunden_ref) = a."Legacy_Customer_ID__c"
