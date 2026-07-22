{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    SPLIT_PART(c.full_name, ' ', 1) AS "FirstName",
    COALESCE(
        NULLIF(SUBSTR(c.full_name, POSITION(' ' IN c.full_name) + 1), ''),
        INITCAP(c.full_name)
    ) AS "LastName",
    LOWER(TRIM(c.email)) AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    CASE UPPER(TRIM(c.company_name))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
    END AS "Role__c",
    NULL AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON c.account_ref = a.id