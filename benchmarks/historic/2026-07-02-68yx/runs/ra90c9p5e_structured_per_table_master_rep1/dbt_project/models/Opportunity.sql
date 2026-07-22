{{ config(materialized='table') }}

WITH raw_opps AS (
    SELECT *
    FROM {{ source('fixture_master_src', 'master_opportunities') }}
),
parsed_dates AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref,

        -- Clean close date: parse multiple formats to ISO YYYY-MM-DD
        CASE
            WHEN zieldatum IS NULL 
                OR TRIM(zieldatum) = 'N/A' 
                OR TRIM(zieldatum) = '0000-00-00' 
                OR TRIM(zieldatum) = '' 
            THEN NULL
            -- Already ISO format (YYYY-MM-DD)
            WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN zieldatum
            -- YYYYMMDD compact format
            WHEN zieldatum ~ '^\d{8}$' THEN 
                TO_DATE(zieldatum, 'YYYYMMDD')::TEXT
            -- MM/DD/YYYY format (US style)
            WHEN zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
                TO_DATE(zieldatum, 'MM/DD/YYYY')::TEXT
            ELSE NULL
        END AS close_date_parsed,

        -- Clean amount: strip currency prefix/text, keep decimal point
        REGEXP_REPLACE(
            COALESCE(NULLIF(TRIM(auftragswert), 'None'), '0'), 
            '^(EUR|USD|GBP|CHF|€|Dollar|£|SFR)\s*', '', 
            'i'
        ) AS amount_cleaned

    FROM raw_opps
),
final AS (
    SELECT
        opp_kennung AS "Id",
        COALESCE(TRIM(titel), 'Unnamed Opportunity') AS "Name",

        -- Map vertriebsphase to Salesforce stage names
        CASE UPPER(TRIM(vertriebsphase))
            WHEN 'PROSPECTING' THEN 'Prospecting'
            WHEN 'PROSPECT' THEN 'Prospecting'
            WHEN 'IN KONTAKT' THEN 'Prospecting'
            
            WHEN 'QUALIFICATION' THEN 'Qualification'
            WHEN 'QUALI' THEN 'Qualification'
            WHEN 'QUALIFIKATION' THEN 'Qualification'
            WHEN 'IN PRÜFUNG' THEN 'Needs Analysis'
            
            WHEN 'BEDARFSANALYSE' THEN 'Needs Analysis'
            
            WHEN 'WERTVONTRAG' THEN 'Value Proposition'
            WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
            WHEN 'MEHRWERTPROPOSITION' THEN 'Value Proposition'
            
            WHEN 'ID. ENTSCHEIDER' THEN 'Id. Decision Makers'
            WHEN 'IDENTIFY DECISION MAKERS' THEN 'Id. Decision Makers'
            WHEN 'ENTSCHEIDER' THEN 'Id. Decision Makers'
            
            WHEN 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
            WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
            
            WHEN 'ANGEBOT/PREISANGABE' THEN 'Proposal/Price Quote'
            WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
            WHEN 'BIETUNG' THEN 'Proposal/Price Quote'
            
            WHEN 'VERHANDLUNG/ÜBERPRÜFUNG' THEN 'Negotiation/Review'
            WHEN 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
            
            -- Closed Won variants
            WHEN 'GEWONNEN' THEN 'Closed Won'
            WHEN 'WON' THEN 'Closed Won'
            WHEN 'CLOSED WON' THEN 'Closed Won'
            WHEN 'ABGESCHLOSSEN (GEWONNEN)' THEN 'Closed Won'
            
            -- Closed Lost variants
            WHEN 'VERLOREN' THEN 'Closed Lost'
            WHEN 'LOST' THEN 'Closed Lost'
            WHEN 'CLOSED LOST' THEN 'Closed Lost'
            WHEN 'ABGESCHLOSSEN (VERLOREN)' THEN 'Closed Lost'
            
            ELSE 'Prospecting'
        END AS "StageName",

        close_date_parsed AS "CloseDate",

        -- Parse amount: US-style decimal (dot is decimal separator)
        CASE 
            WHEN amount_cleaned ~ '^[+-]?\d+\.\d+$' THEN
                amount_cleaned::DOUBLE PRECISION
            WHEN amount_cleaned ~ '^[+-]?\d+$' THEN
                amount_cleaned::DOUBLE PRECISION
            ELSE NULL
        END AS "Amount",

        -- Normalize currency code to ISO 3-digit codes
        CASE UPPER(TRIM(COALESCE(waehrungscode, '')))
            WHEN 'EUR' THEN 'EUR'
            WHEN '€' THEN 'EUR'
            WHEN 'USD' THEN 'USD'
            WHEN 'DOLLAR' THEN 'USD'
            WHEN '$' THEN 'USD'
            WHEN 'GBP' THEN 'GBP'
            WHEN '£' THEN 'GBP'
            WHEN 'CHF' THEN 'CHF'
            WHEN 'SFR' THEN 'CHF'
            WHEN '₣' THEN 'CHF'
            ELSE NULL
        END AS "CurrencyIsoCode",

        kunden_ref AS "AccountId",
        opp_kennung AS "Legacy_Opportunity_ID__c",
        
        NULL AS "CreatedDate",
        NULL AS "LastModifiedDate",
        0 AS "IsDeleted"

    FROM parsed_dates
)
SELECT * FROM final