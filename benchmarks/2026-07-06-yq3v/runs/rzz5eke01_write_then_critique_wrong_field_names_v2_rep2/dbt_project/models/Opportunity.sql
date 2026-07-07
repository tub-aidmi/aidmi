{{ config(materialized='table') }}

SELECT
    chance.chance_id AS "Id",
    chance.bezeichnung AS "Name",
    CASE
        WHEN chance.phase ILIKE '%prospect%' THEN 'Prospecting'
        WHEN chance.phase ILIKE '%qualif%' THEN 'Qualification'
        WHEN chance.phase ILIKE '%needs analy%' THEN 'Needs Analysis'
        WHEN chance.phase ILIKE '%value prop%' THEN 'Value Proposition'
        WHEN chance.phase ILIKE '%decision maker%' THEN 'Id. Decision Makers'
        WHEN chance.phase ILIKE '%perception analy%' THEN 'Perception Analysis'
        WHEN chance.phase ILIKE '%proposal%' OR chance.phase ILIKE '%price quote%' THEN 'Proposal/Price Quote'
        WHEN chance.phase ILIKE '%negotiation%' OR chance.phase ILIKE '%review%' THEN 'Negotiation/Review'
        WHEN chance.phase ILIKE '%won%' THEN 'Closed Won'
        WHEN chance.phase ILIKE '%lost%' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    COALESCE(
        (SELECT TO_CHAR(TO_DATE(chance.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD') WHERE chance.abschlussdatum ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'),
        (SELECT TO_CHAR(TO_DATE(chance.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD') WHERE chance.abschlussdatum ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{4}$'),
        (SELECT TO_CHAR(TO_DATE(chance.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD') WHERE chance.abschlussdatum ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'),
        (SELECT TO_CHAR(TO_DATE(chance.abschlussdatum, 'MM-DD-YYYY'), 'YYYY-MM-DD') WHERE chance.abschlussdatum ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'),
        (SELECT TO_CHAR(TO_DATE(chance.abschlussdatum, 'YYYYMMDD'), 'YYYY-MM-DD') WHERE chance.abschlussdatum ~ '^[0-9]{8}$'),
        '1900-01-01' -- Default if all parsing fails, for NOT NULL target column
    ) AS "CloseDate",
    chance.volumen AS "Amount",
    chance.waehrung AS "CurrencyIsoCode",
    chance.kd_nr AS "AccountId",
    chance.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chance