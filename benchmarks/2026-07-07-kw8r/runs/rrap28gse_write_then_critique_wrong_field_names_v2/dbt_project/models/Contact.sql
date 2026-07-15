{{ config(materialized='table') }}

SELECT 
    ap."ap_id" AS "Id",
    INITCAP(TRIM(ap."vorname")) AS "FirstName",
    COALESCE(INITCAP(TRIM(ap."nachname")), 'Unknown') AS "LastName",
    LOWER(TRIM(ap."email_adresse")) AS "Email",
    TRIM(ap."telefonnummer") AS "Phone",
    INITCAP(TRIM(ap."position")) AS "Title",
    CASE UPPER(TRIM(ap."funktion"))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER'       THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(ap."sprache"))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
     '001' || REGEXP_REPLACE(ap."kunde", '\D', '', 'g') AS "AccountId",
    ap."ap_id" AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap