{{ config(materialized='table') }}

SELECT
    MD5(kontakte.kontakt_id) AS "Id",
    kontakte.rufname AS "FirstName",
    COALESCE(kontakte.familienname, 'Unknown') AS "LastName",
    kontakte.kontakt_email AS "Email",
    kontakte.tel AS "Phone",
    kontakte.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(kontakte.rolle) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(kontakte.rolle) = 'end user' THEN 'End User'
        WHEN LOWER(kontakte.rolle) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(kontakte.rolle) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(kontakte.korrespondenzsprache) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(kontakte.korrespondenzsprache)
        ELSE NULL
    END AS "Preferred_Language__c",
    account."Id" AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakte
LEFT JOIN
    {{ ref('Account') }} AS account
ON
    kontakte.kd_nummer = account."Legacy_Customer_ID__c"