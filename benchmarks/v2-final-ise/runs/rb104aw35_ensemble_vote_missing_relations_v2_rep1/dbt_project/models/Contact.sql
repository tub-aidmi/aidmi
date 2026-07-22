{{ config(materialized='table') }}

SELECT
    CAST(c.id AS TEXT) AS "Id",
    INITCAP(NULLIF(split_part(c.full_name, ' ', 1), '')) AS "FirstName",
    COALESCE(
        INITCAP(trim(substr(c.full_name, position(' ' IN c.full_name) + 1))),
        'Unknown'
    ) AS "LastName",
    CAST(c.email AS TEXT) AS "Email",
    NULL::TEXT AS "Phone",
    NULL::TEXT AS "Title",
    NULL::TEXT AS "Role__c",
    NULL::TEXT AS "Preferred_Language__c",
    CAST(c.account_ref AS TEXT) AS "AccountId",
    CAST(c.id AS TEXT) AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c