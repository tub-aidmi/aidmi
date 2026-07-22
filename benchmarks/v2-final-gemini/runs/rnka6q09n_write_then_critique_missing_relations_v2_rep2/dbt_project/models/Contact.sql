{{ config(materialized='table') }}

WITH cleaned_contacts AS (
    SELECT
        id,
        TRIM(REGEXP_REPLACE(full_name, '\s+', ' ', 'g')) AS cleaned_full_name,
        email,
        account_ref,
        company_name
    FROM
        {{ source('fixture_missing_relations_v2_src', 'contact') }}
)
SELECT
    c.id AS "Id",
    CASE
        WHEN POSITION(' ' IN c.cleaned_full_name) > 0 THEN SPLIT_PART(c.cleaned_full_name, ' ', 1)
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        CASE
            WHEN POSITION(' ' IN c.cleaned_full_name) > 0 THEN TRIM(SUBSTRING(c.cleaned_full_name FROM POSITION(' ' IN c.cleaned_full_name) + 1))
            ELSE c.cleaned_full_name
        END,
        'UNKNOWN'
    ) AS "LastName",
    c.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_contacts AS c
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    c.account_ref = a.id