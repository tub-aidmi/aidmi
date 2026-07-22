-- noinspection SqlNoDataSourceInspectionForFile

{{ config(materialized='table') }}

SELECT
    MD5(o.opp_kennung) AS "Id",
    COALESCE(TRIM(o.titel), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN TRIM(o.vertriebsphase) = 'Interessenbekundung' THEN 'Prospecting'
        WHEN TRIM(o.vertriebsphase) = 'Qualifizierung' THEN 'Qualification'
        WHEN TRIM(o.vertriebsphase) = 'Bedarfsanalyse' THEN 'Needs Analysis'
        WHEN TRIM(o.vertriebsphase) = 'Lösungspräsentation' THEN 'Value Proposition'
        WHEN TRIM(o.vertriebsphase) = 'Entscheider Identifiziert' THEN 'Id. Decision Makers'
        WHEN TRIM(o.vertriebsphase) = 'Wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN TRIM(o.vertriebsphase) = 'Angebotsphase' THEN 'Proposal/Price Quote'
        WHEN TRIM(o.vertriebsphase) = 'Verhandlungsphase' THEN 'Negotiation/Review'
        WHEN TRIM(o.vertriebsphase) = 'Abgeschlossen - Gewonnen' THEN 'Closed Won'
        WHEN TRIM(o.vertriebsphase) = 'Abgeschlossen - Verloren' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to Prospecting as StageName is NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(CAST(o.zieldatum AS DATE), 'YYYY-MM-DD'), -- Handles YYYY-MM-DD
        TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD'),
        TO_CHAR(NOW(), 'YYYY-MM-DD') -- Fallback if date parsing fails, as CloseDate is NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN o.auftragswert ~ '^[0-9]+([\\.,][0-9]+)?$' THEN
            REPLACE(REPLACE(o.auftragswert, '.', ''), ',', '.')::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    TRIM(o.waehrungscode) AS "CurrencyIsoCode",
    MD5(o.kunden_ref) AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    NOW() AS "CreatedDate",
    NOW() AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o