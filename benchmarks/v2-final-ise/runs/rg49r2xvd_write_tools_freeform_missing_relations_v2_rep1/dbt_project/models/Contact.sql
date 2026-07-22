{{ config(materialized='table') }}

-- Contact model: split full_name into FirstName/LastName, map account_ref to Account Id
with source as (
    select *
    from {{ source('fixture_missing_relations_v2_src', 'contact') }}
),

parsed as (
    select
        -- Id: contact primary key
        "id" AS "Id",

        -- FirstName: first word of full_name
        CASE
            WHEN COALESCE("full_name", '') = '' THEN NULL
            ELSE SPLIT_PART(TRIM("full_name"), ' ', 1)
        END AS "FirstName",

        -- LastName: everything after the first space in full_name
        CASE
            WHEN COALESCE("full_name", '') = '' THEN 'Unknown'
            WHEN TRIM("full_name") NOT LIKE '% %' THEN TRIM("full_name")
            ELSE TRIM(SUBSTRING(TRIM("full_name") FROM POSITION(' ' IN TRIM("full_name")) + 1))
        END AS "LastName",

        -- Email
        LOWER(TRIM(COALESCE("email", ''))) AS "Email",

        -- Phone: not in source, default NULL
        NULL AS "Phone",

        -- Title: not in source, default NULL
        NULL AS "Title",

        -- Role__c: default to 'End User' since no role column exists in source
        'End User' AS "Role__c",

        -- Preferred_Language__c: default to 'EN' since no language column exists
        'EN' AS "Preferred_Language__c",

        -- AccountId: map contact.account_ref to the format of account.id.
        -- Use a direct mapping assuming account_ref values correspond to account ids.
        CASE
            WHEN COALESCE(TRIM("account_ref"), '') = '' THEN NULL
            ELSE TRIM("account_ref")
        END AS "AccountId",

        -- Legacy_Contact_ID__c: from source natural key
        "id" AS "Legacy_Contact_ID__c",

        -- CreatedDate: not in source, default NULL
        NULL AS "CreatedDate",

        -- LastModifiedDate: not in source, default NULL
        NULL AS "LastModifiedDate",

        -- IsDeleted: default 0 (active)
        0 AS "IsDeleted"

    from source
)

select * from parsed
