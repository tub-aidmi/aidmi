{{ config(materialized='table') }}

SELECT
    -- Id: Map source opp_kennung to Salesforce-style ID using deterministic hash, consistent with Project__c.Opportunity__c
    '006' || LEFT(LOWER(MD5(TRIM(opp_kennung))), 14) AS "Id",

    -- Name: Opportunity title, cleaned and capitalized; COALESCE to satisfy NOT NULL constraint
    INITCAP(TRIM(COALESCE(titel, 'Unnamed Opportunity'))) AS "Name",

    -- StageName: Map German sales phases to Salesforce stages; ELSE maps to 'Prospecting' (valid enum member) to satisfy NOT NULL constraint
    CASE LOWER(TRIM(vertriebsphase))
        WHEN 'vertrieb' THEN 'Prospecting'
        WHEN 'akquise' THEN 'Qualification'
        WHEN '1 - bedarfsanalyse' THEN 'Needs Analysis'
        WHEN '2 - qualifikation' THEN 'Qualification'
        WHEN '3 - bedarfsanalyse' THEN 'Needs Analysis'
        WHEN 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN 'wert proposition' THEN 'Value Proposition'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN '4 - entscheidungsträger identifizieren' THEN 'Id. Decision Makers'
        WHEN 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN 'angebot/preisanfrage' THEN 'Proposal/Price Quote'
        WHEN 'angebot erstellt' THEN 'Proposal/Price Quote'
        WHEN 'verhandlung/review' THEN 'Negotiation/Review'
        WHEN 'verhandlung' THEN 'Negotiation/Review'
        WHEN 'angebot verhandlung' THEN 'Negotiation/Review'
        WHEN '5 - angebot erstellt' THEN 'Proposal/Price Quote'
        WHEN '6 - verhandlung' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'auftrag erteilt' THEN 'Closed Won'
        WHEN 'abschuss erfolgreich' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        WHEN 'verloren' THEN 'Closed Lost'
        WHEN 'auftrag nicht erteilt' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",

    -- CloseDate: Parse DD.MM.YYYY date format to ISO YYYY-MM-DD; fallback to CURRENT_DATE for missing dates (NOT NULL)
    CASE 
        WHEN zieldatum IS NOT NULL AND TRIM(zieldatum) != '' AND TRIM(zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$' 
            THEN TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY')::DATE::TEXT
        WHEN zieldatum IS NOT NULL AND TRIM(zieldatum) != '' AND TRIM(zieldatum) ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TRIM(zieldatum)
        WHEN zieldatum IS NOT NULL AND TRIM(zieldatum) != '' AND TRIM(zieldatum) ~ '^\d{8}$'
            THEN SUBSTR(TRIM(zieldatum), 1, 4) || '-' || SUBSTR(TRIM(zieldatum), 5, 2) || '-' || SUBSTR(TRIM(zieldatum), 7, 2)
        ELSE CURRENT_DATE::TEXT 
    END AS "CloseDate",

    -- Amount: Clean European number format (dots as thousands, comma as decimal), strip currency prefix/symbols
    CASE 
        WHEN auftragswert IS NOT NULL AND TRIM(auftragswert) != '' THEN
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(TRIM(auftragswert), '^[^\d,.]*', '', 'g'),
                        '\.(\d{3})(?=[\.,]|$)', '', 'g'
                      ),
                    ',', '.'
                   ) AS DOUBLE PRECISION
              )
        ELSE NULL 
    END AS "Amount",

    -- CurrencyIsoCode: Normalize currency code to uppercase
    UPPER(TRIM(waehrungscode)) AS "CurrencyIsoCode",

    -- AccountId: Join to master_kunden; produces Salesforce-style ID consistent with Account.Id formula (deterministic hash)
    '001' || SUBSTRING(MD5(TRIM(mk.kundennummer)) FROM 1 FOR 15) AS "AccountId",

    -- Legacy_Opportunity_ID__c: Store original source natural key for row-level verification
    TRIM(opp_kennung) AS "Legacy_Opportunity_ID__c",

    -- System dates (not in source, use placeholder)
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} opp
    
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk
    ON TRIM(opp.kunden_ref) = TRIM(mk.kundennummer)