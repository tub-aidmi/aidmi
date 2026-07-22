{{ config(materialized='table') }}

SELECT
    opp.opp_kennung AS "Id",
    COALESCE(opp.titel, 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN opp.vertriebsphase ILIKE 'Prospekt%' THEN 'Prospecting'
        WHEN opp.vertriebsphase ILIKE 'Qualifiz%' THEN 'Qualification'
        WHEN opp.vertriebsphase ILIKE 'Bedarfsanalyse' THEN 'Needs Analysis'
        WHEN opp.vertriebsphase ILIKE 'Wertangebot' THEN 'Value Proposition'
        WHEN opp.vertriebsphase ILIKE 'Entscheider identifiziert' THEN 'Id. Decision Makers'
        WHEN opp.vertriebsphase ILIKE 'Wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN opp.vertriebsphase ILIKE 'Vorschlag%' OR opp.vertriebsphase ILIKE 'Angebot%' THEN 'Proposal/Price Quote'
        WHEN opp.vertriebsphase ILIKE 'Verhandlung%' THEN 'Negotiation/Review'
        WHEN opp.vertriebsphase ILIKE 'Gewonnen' THEN 'Closed Won'
        WHEN opp.vertriebsphase ILIKE 'Verloren' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN opp.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(opp.zieldatum AS DATE), 'YYYY-MM-DD')
            WHEN opp.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default if unparseable and NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN opp.auftragswert IS NULL OR TRIM(opp.auftragswert) = '' THEN NULL
        ELSE CAST(REPLACE(REPLACE(REGEXP_REPLACE(TRIM(opp.auftragswert), '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') AS DOUBLE PRECISION)
    END AS "Amount",
    opp.waehrungscode AS "CurrencyIsoCode",
    opp.kunden_ref AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opp