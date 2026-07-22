{{ config(materialized='table') }}

WITH cleaned_contact AS (
    SELECT
        id,
        TRIM(full_name) AS cleaned_full_name,
        email,
        account_ref
    FROM
        {{ source('fixture_missing_relations_v2_src', 'contact') }}
)
SELECT
    cc.id AS "Id",
    CASE
        WHEN cc.cleaned_full_name IS NULL THEN NULL
        WHEN POSITION(' ' IN cc.cleaned_full_name) > 0
        THEN SUBSTRING(cc.cleaned_full_name FROM 1 FOR POSITION(' ' IN cc.cleaned_full_name) - 1)
        ELSE NULL
    END AS "FirstName",
    CASE
        WHEN cc.cleaned_full_name IS NULL THEN 'Unknown' -- Meaningful default for missing full_name
        WHEN POSITION(' ' IN cc.cleaned_full_name) > 0
        THEN SUBSTRING(cc.cleaned_full_name FROM POSITION(' ' IN cc.cleaned_full_name) + 1)
        ELSE cc.cleaned_full_name -- Single word name becomes the LastName
    END AS "LastName",
    cc.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    acc.id AS "AccountId",
    cc.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_contact AS cc
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
ON
    cc.account_ref = acc.id
