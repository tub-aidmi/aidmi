{{ config(materialized='table') }}

SELECT
    opportunity.id AS "Id",
    COALESCE(TRIM(opportunity.name), 'Unknown Opportunity') AS "Name",
    COALESCE(
        CASE UPPER(TRIM(opportunity.stagename))
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
            ELSE 'Prospecting' -- Default to a valid enum value if source is invalid or NULL
        END,
        'Prospecting'
    ) AS "StageName",
    COALESCE(
        CASE
            -- YYYY-MM-DD format
            WHEN opportunity.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN opportunity.closedate
            -- MM/DD/YYYY format
            WHEN opportunity.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opportunity.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            -- DD.MM.YYYY format
            WHEN opportunity.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opportunity.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            -- YYYYMMDD format
            WHEN opportunity.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opportunity.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default to current date if unparseable or NULL
    ) AS "CloseDate",
    COALESCE(
        (SELECT
            CASE
                -- European format: dot for thousand, comma for decimal (e.g., 1.234.567,89)
                WHEN cleaned_amount ~ '^-?\d{1,3}(\.\d{3})*,\d+$' THEN
                    REPLACE(REPLACE(cleaned_amount, '.', ''), ',', '.')::DOUBLE PRECISION
                -- US format: comma for thousand, dot for decimal (e.g., 1,234,567.89)
                WHEN cleaned_amount ~ '^-?\d{1,3}(,\d{3})*\.\d+$' THEN
                    REPLACE(cleaned_amount, ',', '')::DOUBLE PRECISION
                -- Simple European decimal: comma as decimal (e.g., 123,45)
                WHEN cleaned_amount ~ '^-?\d+,\d+$' THEN
                    REPLACE(cleaned_amount, ',', '.')::DOUBLE PRECISION
                -- Simple US decimal: dot as decimal (e.g., 123.45)
                WHEN cleaned_amount ~ '^-?\d+\.\d+$' THEN
                    cleaned_amount::DOUBLE PRECISION
                -- Integer
                WHEN cleaned_amount ~ '^-?\d+$' THEN
                    cleaned_amount::DOUBLE PRECISION
                ELSE NULL
            END
        FROM (
            SELECT
                -- Remove any characters not digits, dots, commas, or leading/trailing +/-
                REGEXP_REPLACE(
                    TRIM(opportunity.amount),
                    '[^0-9.,+-]', '', 'g'
                ) AS cleaned_amount
        ) AS sub_clean),
        NULL
    ) AS "Amount",
    opportunity.currencyisocode AS "CurrencyIsoCode",
    opportunity.accountid AS "AccountId",
    opportunity.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS opportunity
