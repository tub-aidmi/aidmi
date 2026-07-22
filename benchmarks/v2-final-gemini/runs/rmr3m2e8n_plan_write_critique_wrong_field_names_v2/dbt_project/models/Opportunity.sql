{{ config(materialized='table') }}

SELECT
    TRIM(chance_id) AS "Id",
    COALESCE(TRIM(bezeichnung), 'Default Opportunity Name') AS "Name",
    COALESCE(
        CASE TRIM(UPPER(phase))
            WHEN 'PROSPEKTIERUNG' THEN 'Prospecting'
            WHEN 'QUALIFIZIERUNG' THEN 'Qualification'
            WHEN 'BEDARFSANALYSE' THEN 'Needs Analysis'
            WHEN 'WERTPROPOSITION' THEN 'Value Proposition'
            WHEN 'ENTSCHEIDERIDENTIFIKATION' THEN 'Id. Decision Makers'
            WHEN 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
            WHEN 'ANGEBOT/PREISANGEBOT' THEN 'Proposal/Price Quote'
            WHEN 'VERHANDLUNG/ÜBERPRÜFUNG' THEN 'Negotiation/Review'
            WHEN 'GEWONNEN' THEN 'Closed Won'
            WHEN 'VERLOREN' THEN 'Closed Lost'
            ELSE NULL
        END,
        'Prospecting'
    ) AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        '1900-01-01'
    ) AS "CloseDate",
    volumen AS "Amount",
    TRIM(UPPER(waehrung)) AS "CurrencyIsoCode",
    TRIM(kd_nr) AS "AccountId",
    TRIM(chance_id) AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
