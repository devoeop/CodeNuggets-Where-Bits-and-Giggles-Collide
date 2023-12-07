CREATE TABLE total_sales_per_country AS
SELECT
    co.country AS country,
    COALESCE(SUM(f.amount), 0) AS totalsales
FROM
    public."softcartFactSales" f
FULL JOIN
    public."softcartDimCountry" co ON f.countryid = co.countryid
GROUP BY
    co.country;