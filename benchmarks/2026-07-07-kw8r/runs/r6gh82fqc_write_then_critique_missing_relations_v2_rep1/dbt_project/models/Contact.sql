{{ config(materialized='table') }}

WITH contact_source AS (
    SELECT
        id,
        full_name,
        email,
        account_ref,
        company_name
    FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}
),
account_lookup AS (
    SELECT
        TRIM(id) AS account_id,
        INITCAP(TRIM(name)) AS account_name
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
)

SELECT
    -- Contact Id: standardized with TRIM() for cross-table consistency with Account model
    COALESCE(TRIM(contact_source.id), '') AS "Id",

    -- Parse FirstName from full_name: handle both "Last, First" and "First Last" formats
    CASE
        WHEN contact_source.full_name ~ ',\s*' THEN
            INITCAP(TRIM(SPLIT_PART(contact_source.full_name, ',', 2)))
        WHEN position(' ' IN contact_source.full_name) > 0 THEN
            INITCAP(TRIM(SUBSTRING(contact_source.full_name FROM 1 FOR position(' ' IN contact_source.full_name) - 1)))
        ELSE NULL
    END AS "FirstName",

    -- Parse LastName from full_name: default to 'Unknown' when full_name is missing
    COALESCE(
        CASE
            WHEN contact_source.full_name ~ ',\s*' THEN
                INITCAP(TRIM(SPLIT_PART(contact_source.full_name, ',', 1)))
            WHEN position(' ' IN contact_source.full_name) > 0 THEN
                INITCAP(TRIM(SUBSTRING(contact_source.full_name FROM position(' ' IN contact_source.full_name) + 1)))
            ELSE INITCAP(TRIM(contact_source.full_name))
        END,
        'Unknown'
    ) AS "LastName",

    -- Email: lowercase and trim for consistency
    LOWER(TRIM(contact_source.email)) AS "Email",

    -- Phone: not available in source data
    NULL::TEXT AS "Phone",

    -- Title: not available in source data
    NULL::TEXT AS "Title",

    -- Role__c: map company_name; unmapped values default to NULL (aligned with Opportunity and Project models)
    CASE
        WHEN LOWER(TRIM(contact_source.company_name)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(contact_source.company_name)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(contact_source.company_name)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(contact_source.company_name)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",

    -- Preferred_Language__c: not available in source data
    NULL::TEXT AS "Preferred_Language__c",

    -- AccountId: join with account table using TRIMmed keys on both sides to prevent whitespace mismatches
    account_lookup.account_id AS "AccountId",

    -- Legacy_Contact_ID__c: preserve exact source natural key without transformation
    contact_source.id AS "Legacy_Contact_ID__c",

    -- CreatedDate: not available in source data
    NULL::TEXT AS "CreatedDate",

    -- LastModifiedDate: not available in source data
    NULL::TEXT AS "LastModifiedDate",

    -- IsDeleted: default to 0 (not deleted)
    0 AS "IsDeleted"

FROM contact_source
LEFT JOIN account_lookup
    ON TRIM(contact_source.account_ref) = account_lookup.account_id