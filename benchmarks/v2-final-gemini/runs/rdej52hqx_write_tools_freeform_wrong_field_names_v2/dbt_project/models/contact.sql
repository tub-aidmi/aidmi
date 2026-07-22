{{ config(materialized='table') }}

SELECT
    ans.ap_id AS "Id",
    ans.vorname AS "FirstName",
    ans.nachname AS "LastName",
    ans.email_adresse AS "Email",
    ans.telefonnummer AS "Phone",
    ans.position AS "Title",
    CASE
        WHEN ans.funktion IN ('Decision Maker', 'End User', 'Technical Contact', 'Executive Sponsor') THEN ans.funktion
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN ans.sprache IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN ans.sprache
        ELSE NULL
    END AS "Preferred_Language__c",
    ans.kunde AS "AccountId", -- This is kunden_nr from kunden, which is the Account.Id
    ans.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ans
