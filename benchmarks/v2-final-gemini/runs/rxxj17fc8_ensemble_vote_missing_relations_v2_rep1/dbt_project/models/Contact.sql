SELECT
    src_contact.id AS "Id",
    CASE
        WHEN TRIM(src_contact.full_name) IS NULL OR TRIM(src_contact.full_name) = '' THEN NULL
        WHEN POSITION(' ' IN TRIM(src_contact.full_name)) = 0 THEN NULL
        ELSE SPLIT_PART(TRIM(src_contact.full_name), ' ', 1)
    END AS "FirstName",
    CASE
        WHEN TRIM(src_contact.full_name) IS NULL OR TRIM(src_contact.full_name) = '' THEN 'Unknown'
        WHEN POSITION(' ' IN TRIM(src_contact.full_name)) = 0 THEN TRIM(src_contact.full_name)
        ELSE SUBSTRING(TRIM(src_contact.full_name) FROM POSITION(' ' IN TRIM(src_contact.full_name)) + 1)
    END AS "LastName",
    src_contact.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    src_account.id AS "AccountId",
    src_contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS src_contact
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS src_account
    ON src_contact.account_ref = src_account.id