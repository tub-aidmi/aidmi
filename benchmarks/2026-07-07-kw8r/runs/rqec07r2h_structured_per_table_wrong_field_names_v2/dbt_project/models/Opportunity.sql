{{ config(materialized='table') }}

WITH mapped_accounts AS (
    SELECT 
        kunden_nr,
        CONCAT('001', LEFT(MD5(kunden_nr), 14)) AS account_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
),
opportunities_raw AS (
    SELECT
        ch.*,
        ma.account_id AS account_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} ch
    LEFT JOIN mapped_accounts ma ON ch.kd_nr = ma.kunden_nr
)

SELECT 
    CONCAT('006', LEFT(MD5(opp.chance_id), 14)) AS "Id",
    COALESCE(TRIM(opp.bezeichnung), 'Unnamed Opportunity') AS "Name",
    CASE LOWER(TRIM(opp.phase))
        WHEN 'neukunde' THEN 'Prospecting'
        WHEN 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN 'qualifikation' THEN 'Qualification'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'faktorenanalyse' THEN 'Perception Analysis'
        WHEN 'decision makers' THEN 'Id. Decision Makers'
        WHEN 'angebot' THEN 'Proposal/Price Quote'
        WHEN 'verhandlung' THEN 'Negotiation/Review'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'abgewiesen' THEN 'Closed Lost'
        WHEN 'verloren' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        CASE 
            WHEN opp.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(opp.abschlussdatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN opp.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(opp.abschlussdatum)
            ELSE NULL 
        END,
        '1900-01-01'
    ) AS "CloseDate",
    opp.volumen AS "Amount",
    UPPER(TRIM(COALESCE(opp.waehrung, 'EUR'))) AS "CurrencyIsoCode",
    opp.account_id AS "AccountId",
    opp.chance_id AS "Legacy_Opportunity_ID__c",
    CAST(NOW() AS TEXT) AS "CreatedDate",
    CAST(NOW() AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunities_raw opp