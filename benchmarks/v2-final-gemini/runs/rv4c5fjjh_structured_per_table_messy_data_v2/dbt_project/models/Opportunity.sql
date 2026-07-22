-- depends_on: {{ ref('opportunity') }}
{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(TRIM(o.name), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN o.stagename IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost') THEN o.stagename
        ELSE 'Prospecting' -- Default to a valid enum value if source is NULL or invalid
    END AS "StageName",
    COALESCE(
        (
            SELECT d::TEXT
            FROM (
                SELECT CASE
                    WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(o.closedate, 'YYYY-MM-DD')
                    WHEN o.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(o.closedate, 'MM/DD/YYYY')
                    WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(o.closedate, 'DD.MM.YYYY')
                    ELSE NULL
                END AS d
            ) AS parsed_date
        ),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default date if parsing fails
    ) AS "CloseDate",
    CASE
        WHEN TRIM(o.amount) ~ '^\d+(?:[.,]\d+)?$' THEN
            REPLACE(REPLACE(TRIM(o.amount), '.', ''), ',', '.')::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    o.currencyisocode AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS o