{{ config(materialized='table') }}

SELECT
    MD5(opp.opp_kennung) AS "Id",
    TRIM(COALESCE(opp.titel, opp.opp_kennung)) AS "Name",
    CASE UPPER(TRIM(opp.vertriebsphase))
        WHEN 'PROSPECTING' THEN 'Prospecting'
        WHEN 'QUALIFICATION' THEN 'Qualification'
        WHEN 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN 'CLOSED WON' THEN 'Closed Won'
        WHEN 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(TRIM(opp.zieldatum), 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(opp.zieldatum), 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(opp.zieldatum), 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "CloseDate",
    CAST(REPLACE(REPLACE(TRIM(REPLACE(opp.auftragswert, '€', '')), '.', ''), ',', '.') AS DOUBLE PRECISION) AS "Amount",
    UPPER(TRIM(opp.waehrungscode)) AS "CurrencyIsoCode",
    MD5(opp.kunden_ref) AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opp
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS knd
ON
    opp.kunden_ref = knd.kundennummer
