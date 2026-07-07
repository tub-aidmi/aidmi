-- models/Opportunity.sql
{{ config(materialized='table') }}

SELECT
    MD5(opp.opp_kennung) AS "Id",
    COALESCE(opp.titel, 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN opp.vertriebsphase = 'Initialkontakt' THEN 'Prospecting'
        WHEN opp.vertriebsphase = 'Qualifizierung' THEN 'Qualification'
        WHEN opp.vertriebsphase = 'Bedarfsanalyse' THEN 'Needs Analysis'
        WHEN opp.vertriebsphase = 'Wertangebot' THEN 'Value Proposition'
        WHEN opp.vertriebsphase = 'Entscheider Identifizierung' THEN 'Id. Decision Makers'
        WHEN opp.vertriebsphase = 'Wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN opp.vertriebsphase = 'Angebots-/Preisanfrage' THEN 'Proposal/Price Quote'
        WHEN opp.vertriebsphase = 'Verhandlung/Überprüfung' THEN 'Negotiation/Review'
        WHEN opp.vertriebsphase = 'Geschlossen Gewonnen' THEN 'Closed Won'
        WHEN opp.vertriebsphase = 'Geschlossen Verloren' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target
    END AS "StageName",
    COALESCE(
        CASE
            WHEN opp.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            ELSE '1900-01-01' -- Default for unparseable formats
        END,
        '1900-01-01' -- Default if zieldatum itself is NULL
    ) AS "CloseDate",
    CAST(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(opp.auftragswert, '[^0-9,.-]', '', 'g'),
                '\.', '', 'g' -- Remove thousand separators (dots)
            ),
            ',', '.', 'g' -- Replace comma with dot for decimal
        ) AS DOUBLE PRECISION
    ) AS "Amount",
    opp.waehrungscode AS "CurrencyIsoCode",
    MD5(opp.kunden_ref) AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opp