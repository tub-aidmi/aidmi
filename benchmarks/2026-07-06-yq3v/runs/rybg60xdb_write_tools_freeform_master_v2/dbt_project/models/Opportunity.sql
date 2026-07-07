-- models/Opportunity.sql

{{ config(materialized='table') }}

SELECT
    TRIM(opp_kennung) AS "Id",
    COALESCE(TRIM(titel), 'Unknown Opportunity') AS "Name",
    CASE UPPER(TRIM(vertriebsphase))
        WHEN 'PROSPEKTIERUNG' THEN 'Prospecting'
        WHEN 'QUALIFIZIERUNG' THEN 'QUALIFICATION'
        WHEN 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN 'WERTVORSCHLAG' THEN 'Value Proposition'
        WHEN 'ID. ENTSCHEIDER' THEN 'Id. Decision Makers'
        WHEN 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN 'ANGEBOT/PREISANGEBOT' THEN 'Proposal/Price Quote'
        WHEN 'VERHANDLUNG/ÜBERPRÜFUNG' THEN 'Negotiation/Review'
        WHEN 'ABGESCHLOSSEN GEWONNEN' THEN 'Closed Won'
        WHEN 'ABGESCHLOSSEN VERLOREN' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target column
    END AS "StageName",
    COALESCE(
        TO_CHAR(CASE WHEN TRIM(zieldatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(zieldatum), 'YYYY-MM-DD') ELSE NULL END, 'YYYY-MM-DD'),
        TO_CHAR(CASE WHEN TRIM(zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY') ELSE NULL END, 'YYYY-MM-DD'),
        TO_CHAR(CASE WHEN TRIM(zieldatum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(zieldatum), 'MM/DD/YYYY') ELSE NULL END, 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL target column
    ) AS "CloseDate",
    CASE
        WHEN TRIM(auftragswert) ~ '^[0-9]+([\.][0-9]{3})*(\,[0-9]+)?$' THEN
            REPLACE(REPLACE(TRIM(auftragswert), '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN TRIM(auftragswert) ~ '^[0-9]+(\,[0-9]+)?$' THEN
            REPLACE(TRIM(auftragswert), ',', '.')::DOUBLE PRECISION
        WHEN TRIM(auftragswert) ~ '^[0-9]+(\.([0-9]{2}))?$' THEN
            TRIM(auftragswert)::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    TRIM(waehrungscode) AS "CurrencyIsoCode",
    TRIM(kunden_ref) AS "AccountId",
    TRIM(opp_kennung) AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }}
