{{ config(materialized='table') }}

WITH opportunities_stg AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref
    FROM
        {{ source('fixture_master_v2_src', 'master_opportunities') }}
),

kunden_stg AS (
    SELECT
        kundennummer,
        unternehmensname
    FROM
        {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT
    opp.opp_kennung AS "Id",
    COALESCE(opp.titel, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN opp.vertriebsphase = 'Angebot' THEN 'Proposal/Price Quote'
        WHEN opp.vertriebsphase = 'Verhandlung' THEN 'Negotiation/Review'
        WHEN opp.vertriebsphase = 'Qualifikation' THEN 'Qualification'
        WHEN opp.vertriebsphase = 'Bedürfnisanalyse' THEN 'Needs Analysis'
        WHEN opp.vertriebsphase = 'Wertversprechen' THEN 'Value Proposition'
        WHEN opp.vertriebsphase = 'Geschlossen gewonnen' THEN 'Closed Won'
        WHEN opp.vertriebsphase = 'Geschlossen verloren' THEN 'Closed Lost'
        WHEN opp.vertriebsphase = 'Identifizierung von Entscheidungsträgern' THEN 'Id. Decision Makers'
        WHEN opp.vertriebsphase = 'Wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN opp.vertriebsphase IN ('Interessent', 'Prospektion') THEN 'Prospecting'
        ELSE 'Prospecting' -- Default to Prospecting for unmapped values
    END AS "StageName",
    COALESCE(
        CASE
            WHEN opp.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN opp.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN opp.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL -- For unparseable formats
        END,
        '1900-01-01' -- Default for NOT NULL target
    ) AS "CloseDate",
    CAST(REPLACE(REPLACE(opp.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION) AS "Amount",
    opp.waehrungscode AS "CurrencyIsoCode",
    knd.kundennummer AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    opportunities_stg AS opp
LEFT JOIN
    kunden_stg AS knd ON opp.kunden_ref = knd.kundennummer
