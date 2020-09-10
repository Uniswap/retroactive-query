BEGIN

    CREATE TEMP FUNCTION
        TO_HEX_STRING(amount FLOAT64)
        RETURNS STRING
        LANGUAGE js AS """
        const num = BigInt(Math.floor(amount)) * 10n ** 18n + BigInt(Math.floor((amount - Math.floor(amount)) * (10 ** 18)))
        const truncated = (num / (10n ** 12n)) * (10n ** 12n)
        return `0x${truncated.toString(16)}`;
    """;

    CREATE TABLE all_earnings_hexadecimal AS
    (
    SELECT address,
           TO_HEX_STRING(earnings) as earnings,
           reasons
    FROM all_earnings);

END;