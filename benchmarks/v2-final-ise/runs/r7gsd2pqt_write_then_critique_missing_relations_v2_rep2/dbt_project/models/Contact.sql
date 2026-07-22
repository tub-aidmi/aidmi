{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",

    -- FirstName: first word of full_name using split_part (reviewer fix #1)
    CASE
        WHEN full_name IS NOT NULL AND TRIM(full_name) != ''
            THEN INITCAP(TRIM(SPLIT_PART(full_name, ' ', 1)))
        ELSE NULL
    END AS "FirstName",

    -- LastName: everything after first space in full_name, default Unknown for NOT NULL
    CASE
        WHEN POSITION(' ' IN full_name) > 0
            THEN INITCAP(TRIM(SUBSTRING(full_name FROM POSITION(' ' IN full_name) + 1)))
        ELSE 'Unknown'
    END AS "LastName",

    -- Email: from source email
    CAST(email AS TEXT) AS "Email",

    -- Phone: not available in source, NULL
    NULL::TEXT AS "Phone",

    -- Title: not available in source, NULL
    NULL::TEXT AS "Title",

    -- Role__c: not available in source data, default to NULL (enum fallback)
    NULL::TEXT AS "Role__c",

    -- Preferred_Language__c: not available in source data, default to NULL (enum fallback)
    NULL::TEXT AS "Preferred_Language__c",

    -- AccountId: account_ref already matches ACC-XXXX format from Account.Id (reviewer fix #2 verified via query_postgres)
    CASE
        WHEN account_ref IS NOT NULL AND TRIM(account_ref) != ''
            THEN TRIM(account_ref)
        ELSE NULL
    END AS "AccountId",

    -- Legacy_Contact_ID__c: source natural key
    CAST(id AS TEXT) AS "Legacy_Contact_ID__c",

    -- CreatedDate: not available in source, NULL
    NULL::TEXT AS "CreatedDate",

    -- LastModifiedDate: not available in source, NULL
    NULL::TEXT AS "LastModifiedDate",

    -- IsDeleted: not available in source, default to 0 (not deleted)
    0 AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}