-- depends_on: {{ ref('Account') }}

{{ config(materialized='table') }}

SELECT
    MD5(mo.opp_kennung) AS "Id",
    COALESCE(TRIM(mo.titel), mo.opp_kennung) AS "Name",
    CASE
        WHEN LOWER(mo.vertriebsphase) = 'erste kontaktaufnahme' THEN 'Prospecting'
        WHEN LOWER(mo.vertriebsphase) = 'qualifizierung' THEN 'Qualification'
        WHEN LOWER(mo.vertriebsphase) = 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN LOWER(mo.vertriebsphase) = 'werteversprechen' THEN 'Value Proposition'
        WHEN LOWER(mo.vertriebsphase) = 'entscheider identifizieren' THEN 'Id. Decision Makers'
        WHEN LOWER(mo.vertriebsphase) = 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN LOWER(mo.vertriebsphase) = 'angebotsphase' THEN 'Proposal/Price Quote'
        WHEN LOWER(mo.vertriebsphase) = 'verhandlung' THEN 'Negotiation/Review'
        WHEN LOWER(mo.vertriebsphase) = 'abgeschlossen gewonnen' THEN 'Closed Won'
        WHEN LOWER(mo.vertriebsphase) = 'abgeschlossen verloren' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for unmapped or NULL values, as StageName is NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(mo.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        '2000-01-01' -- Changed from '1900-01-01' to '2000-01-01' as per reviewer comment for NOT NULL date
    ) AS "CloseDate",
    CASE
        WHEN REPLACE(REPLACE(mo.auftragswert, '.', ''), ',', '.') ~ '^-?\d+(\.\d+)?$' THEN
            REPLACE(REPLACE(mo.auftragswert, '.', ''), ',', '.')::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    TRIM(mo.waehrungscode) AS "CurrencyIsoCode",
    acc."Id" AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
LEFT JOIN
    {{ ref('Account') }} AS acc ON mo.kunden_ref = acc."Legacy_Customer_ID__c";