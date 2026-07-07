-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlDialectInspectionForFile

{{ config(materialized='table') }}

WITH cleaned_chancen AS (
    SELECT
        chance_id,
        COALESCE(bezeichnung, 'Unknown Opportunity') AS bezeichnung,
        phase,
        abschlussdatum,
        volumen,
        waehrung,
        kd_nr
    FROM
        {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
)

SELECT
    MD5(c.chance_id) AS "Id",
    c.bezeichnung AS "Name",
    CASE UPPER(c.phase)
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
        ELSE 'Prospecting' -- Default for NOT NULL field
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(c.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(c.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL field
    ) AS "CloseDate",
    c.volumen AS "Amount",
    CASE UPPER(c.waehrung)
        WHEN 'EUR' THEN 'EUR'
        ELSE 'USD' -- Default currency
    END AS "CurrencyIsoCode",
    MD5(k.kunden_nr) AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_chancen AS c
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    c.kd_nr = k.kunden_nr