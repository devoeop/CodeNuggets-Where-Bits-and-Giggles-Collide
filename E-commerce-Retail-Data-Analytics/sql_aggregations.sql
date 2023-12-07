SELECT
    co.country AS country,
    cat.category AS category,
    COALESCE(SUM(f.amount), 0) AS totalsales
FROM
    public."softcartFactSales" f
FULL JOIN
    public."softcartDimCategory" cat ON f.countryid = cat.categoryid
FULL JOIN
    public."softcartDimCountry" co ON f.categoryid = co.countryid
GROUP BY
    GROUPING SETS (
        (country, category),  -- Group by country and category
        (country),             -- Group by country only
        ()                     -- Grand total (no grouping)
    );
	

SELECT
    COALESCE(d.year, 0) AS year,
    COALESCE(co.country, 'null') AS country,
    COALESCE(SUM(f.amount), 0) AS totalsales
FROM
    public."softcartFactSales" f
FULL JOIN
    public."softcartDimCountry" co ON f.countryid = co.countryid
FULL JOIN
    public."softcartDimDate" d ON f.dateid = d.dateid
GROUP BY
    ROLLUP (d.year, co.country)
ORDER BY co.country;


SELECT
    COALESCE(d.year, 0) AS year,
    COALESCE(co.country, 'null') AS country,
    COALESCE(AVG(f.amount), 0) AS average_sales
FROM
    public."softcartFactSales" f
FULL JOIN
    public."softcartDimCountry" co ON f.countryid = co.countryid
FULL JOIN
    public."softcartDimDate" d ON f.dateid = d.dateid
GROUP BY
    CUBE (d.year, co.country)
ORDER BY co.country;