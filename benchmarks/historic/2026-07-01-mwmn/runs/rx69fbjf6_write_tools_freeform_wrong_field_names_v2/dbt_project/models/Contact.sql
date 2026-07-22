{{ config(materialized='table') }}

SELECT
    "ap_id" AS "Id",
    "vorname" AS "FirstName",
    "nachname" AS "LastName",
    "email_adresse" AS "Email",
    "telefonnummer" AS "Phone",
    "position" AS "Title",
    CASE
        WHEN UPPER("funktion") = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER("funktion") = 'END USER' THEN 'End User'
        WHEN UPPER("funktion") = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER("funktion") = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER("sprache") = 'DE' THEN 'DE'
        WHEN UPPER("sprache") = 'EN' THEN 'EN'
        WHEN UPPER("sprache") = 'FR' THEN 'FR'
        WHEN UPPER("sprache") = 'ES' THEN 'ES'
        WHEN UPPER("sprache") = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    "kunde" AS "AccountId",
    "ap_id" AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
