-- depends_on: {{ ref('account') }}

{{ config(materialized='table') }}

SELECT
    opp.opp_kennung AS "Id",
    COALESCE(TRIM(opp.titel), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(opp.vertriebsphase) = 'qualifizierung' THEN 'Qualification'
        WHEN LOWER(opp.vertriebsphase) = 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN LOWER(opp.vertriebsphase) = 'wertangebot' THEN 'Value Proposition'
        WHEN LOWER(opp.vertriebsphase) = 'entscheidungsträger identifiziert' THEN 'Id. Decision Makers'
        WHEN LOWER(opp.vertriebsphase) = 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN LOWER(opp.vertriebsphase) = 'angebotsphase' THEN 'Proposal/Price Quote'
        WHEN LOWER(opp.vertriebsphase) = 'verhandlungsphase' THEN 'Negotiation/Review'
        WHEN LOWER(opp.vertriebsphase) = 'gewonnen' THEN 'Closed Won'
        WHEN LOWER(opp.vertriebsphase) = 'verloren' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for unknown or NULL stages, as StageName is NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN opp.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN opp.zieldatum
            WHEN opp.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN opp.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for unparseable or NULL dates, as CloseDate is NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN opp.auftragswert ~ '^[\d.,\s]+$' THEN
            REPLACE(REPLACE(TRIM(opp.auftragswert), '.', ''), ',', '.')::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    opp.waehrungscode AS "CurrencyIsoCode",
    kunden.kundennummer AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opp
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
    ON opp.kunden_ref = kunden.kundennummer