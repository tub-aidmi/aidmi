{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unnamed Opportunity') AS "Name",
    CASE o.stage
        WHEN 'Prospecting' THEN 'Prospecting'
        WHEN 'Qualification' THEN 'Qualification'
        WHEN 'Needs Analysis' THEN 'Needs Analysis'
        WHEN 'Value Proposition' THEN 'Value Proposition'
        WHEN 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN 'Perception Analysis' THEN 'Perception Analysis'
        WHEN 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN 'Closed Won' THEN 'Closed Won'
        WHEN 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NULL or unmapped values
    END AS "StageName",
    -- CloseDate is NOT NULL in target but not available in source. Defaulting to a placeholder date.
    CAST('1900-01-01' AS TEXT) AS "CloseDate",
    o.amount AS "Amount",
    -- CurrencyIsoCode is not available in source. Defaulting to 'USD'.
    CAST('USD' AS TEXT) AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    -- CreatedDate is not available in source. Defaulting to a placeholder date.
    CAST('1900-01-01' AS TEXT) AS "CreatedDate",
    -- LastModifiedDate is not available in source. Defaulting to a placeholder date.
    CAST('1900-01-01' AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    REPLACE(o.customer_number, 'KD-', 'ACC-') = a.id
