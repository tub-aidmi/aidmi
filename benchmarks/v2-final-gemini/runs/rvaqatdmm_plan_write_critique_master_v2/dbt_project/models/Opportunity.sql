{{ config(materialized='table') }}

SELECT
    MD5(opp.opp_kennung) AS "Id",
    COALESCE(TRIM(opp.titel), 'Unknown Opportunity ' || opp.opp_kennung) AS "Name",
    CASE
        WHEN LOWER(TRIM(opp.vertriebsphase)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(opp.vertriebsphase)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(opp.vertriebsphase)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(opp.vertriebsphase)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('id. decision makers', 'id decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(opp.vertriebsphase)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('proposal/price quote', 'proposal / price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('negotiation/review', 'negotiation / review') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('closed won', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('closed lost', 'lost') THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN opp.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE '1900-01-01'
    END AS "CloseDate",
    CASE
        WHEN opp.auftragswert IS NULL OR TRIM(opp.auftragswert) = '' THEN NULL
        ELSE
            CASE
                WHEN REGEXP_REPLACE(TRIM(opp.auftragswert), '[^0-9,.]', '', 'g') = '' THEN NULL
                ELSE CAST(REPLACE(REPLACE(REGEXP_REPLACE(TRIM(opp.auftragswert), '[^0-9,.]', '', 'g'), '.', ''), ',', '.') AS DOUBLE PRECISION)
            END
    END AS "Amount",
    TRIM(UPPER(opp.waehrungscode)) AS "CurrencyIsoCode",
    MD5(knd.kundennummer) AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opp
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS knd
    ON opp.kunden_ref = knd.kundennummer
