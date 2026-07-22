{{ config(materialized='table') }}

WITH contact_data AS (
    SELECT
        c.id,
        c.full_name,
        c.email,
        c.account_ref,
        c.company_name
    FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
),
account_normalized AS (
    SELECT
        a.id AS account_id,
        a.name AS account_name,
        LOWER(REGEXP_REPLACE(REGEXP_REPLACE(a.name, '[^a-zA-Z0-9 ]', '', 'g'), '\s+', ' ', 'g')) AS normalized_name
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a
),
contact_with_account AS (
    SELECT
        cd.id,
        cd.full_name,
        cd.email,
        cd.account_ref,
        cd.company_name,
        COALESCE(
            -- Try direct match via account_ref
            (SELECT a.account_id FROM account_normalized a WHERE a.account_id = cd.account_ref),
            -- Fallback: match company_name to account.name (normalized)
            (SELECT a.account_id 
             FROM account_normalized a 
             WHERE LOWER(REGEXP_REPLACE(REGEXP_REPLACE(cd.company_name, '[^a-zA-Z0-9 ]', '', 'g'), '\s+', ' ', 'g')) = a.normalized_name 
             LIMIT 1)
        ) AS account_id
    FROM contact_data cd
)

SELECT
    id AS "Id",
    CASE
        WHEN full_name IS NOT NULL AND full_name ~ '\S' THEN
            CASE
                WHEN full_name ~ '^\S+\s+\S+$' THEN TRIM(SPLIT_PART(full_name, ' ', 1))
                ELSE NULL
            END
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        NULLIF(
            CASE
                WHEN full_name IS NOT NULL AND full_name ~ '\S' THEN
                    CASE
                        WHEN full_name ~ '^\S+\s+\S+$' THEN TRIM(SPLIT_PART(full_name, ' ', 2))
                        ELSE TRIM(full_name)
                    END
                ELSE NULL
            END, 
            ''
        ),
        'Unknown'
    ) AS "LastName",
    email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    account_id AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM contact_with_account
