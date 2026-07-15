{{ config(materialized='table') }}

SELECT
    k."kontakt_id" AS "Id",
    TRIM(INITCAP(k."rufname")) AS "FirstName",
    TRIM(INITCAP(k."familienname")) AS "LastName",
    TRIM(k."kontakt_email") AS "Email",
    TRIM(k."tel") AS "Phone",
    TRIM(INITCAP(k."berufsbezeichnung")) AS "Title",
    CASE 
        WHEN TRIM(LOWER(k."rolle")) = 'entscheidungsträger' THEN 'Decision Maker'
        WHEN TRIM(LOWER(k."rolle")) = 'endbenutzer' THEN 'End User'
        WHEN TRIM(LOWER(k."rolle")) = 'technischer kontakt' THEN 'Technical Contact'
        WHEN TRIM(LOWER(k."rolle")) = 'exekutiver sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN TRIM(UPPER(k."korrespondenzsprache")) = 'DE' THEN 'DE'
        WHEN TRIM(UPPER(k."korrespondenzsprache")) = 'EN' THEN 'EN'
        WHEN TRIM(UPPER(k."korrespondenzsprache")) = 'FR' THEN 'FR'
        WHEN TRIM(UPPER(k."korrespondenzsprache")) = 'ES' THEN 'ES'
        WHEN TRIM(UPPER(k."korrespondenzsprache")) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    c."kundennummer" AS "AccountId",
    k."kontakt_id" AS "Legacy_Contact_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c
    ON TRIM(k."kd_nummer") = TRIM(c."kundennummer")