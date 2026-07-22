-- dbt model for Opportunity

{{ config(materialized='table') }}

WITH cleaned_opportunity AS (
    SELECT
        id AS source_id,
        name AS source_name,
        stagename AS source_stagename,
        closedate AS source_closedate,
        amount AS source_amount,
        currencyisocode AS source_currencyisocode,
        accountid AS source_accountid
    FROM
        {{ source('fixture_messy_data_v2_src', 'opportunity') }}
)
SELECT
    source_id AS "Id",
    COALESCE(TRIM(source_name), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN TRIM(source_stagename) IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost')
            THEN TRIM(source_stagename)
        ELSE 'Prospecting' -- Default for NOT NULL enum
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(source_closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(source_closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(source_closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(source_closedate, 'YYYYMMDD'), 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN source_amount IS NULL THEN NULL
        ELSE
            CAST(REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        TRIM(source_amount),
                        '[^0-9.,]+', '', 'g' -- Remove non-numeric except comma and dot
                    ),
                    -- Handle European format (e.g., 1.234,56 -> 1234.56)
                    CASE WHEN POSITION('.' IN TRIM(source_amount)) < POSITION(',' IN TRIM(source_amount)) AND POSITION('.' IN TRIM(source_amount)) > 0 THEN '.' ELSE '@#$%' END,
                    '', 'g'
                ),
                -- Replace comma with dot for decimal if present
                ',',
                '.', 'g'
            ) AS DOUBLE PRECISION)
    END AS "Amount",
    TRIM(source_currencyisocode) AS "CurrencyIsoCode",
    source_accountid AS "AccountId",
    source_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_opportunity;