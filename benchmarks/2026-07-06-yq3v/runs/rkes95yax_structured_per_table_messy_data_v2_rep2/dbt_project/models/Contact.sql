SELECT
    src_contact.id AS "Id",
    src_contact.firstname AS "FirstName",
    COALESCE(src_contact.lastname, 'Unknown') AS "LastName",
    src_contact.email AS "Email",
    src_contact.phone AS "Phone",
    src_contact.title AS "Title",
    CASE
        WHEN LOWER(src_contact.role__c) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(src_contact.role__c) = 'end user' THEN 'End User'
        WHEN LOWER(src_contact.role__c) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(src_contact.role__c) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(src_contact.preferred_language__c)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(src_contact.preferred_language__c)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(src_contact.preferred_language__c)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(src_contact.preferred_language__c)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(src_contact.preferred_language__c)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    src_contact.accountid AS "AccountId",
    src_contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS src_contact