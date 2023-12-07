BEGIN;


CREATE TABLE public."softcartDimDate"
(
    dateid integer NOT NULL,
    date date,
    Year integer,
    Quarter integer,
    QuarterName character(50),
    Month integer,
    Monthname character(50),
    Day integer,
    Weekday integer,
    WeekdayName character(50),
    PRIMARY KEY (dateid)
);

CREATE TABLE public."softcartDimCategory"
(
    categoryid integer NOT NULL,
    category character(50),
    PRIMARY KEY (categoryid)
);

CREATE TABLE public."softcartDimCountry"
(
    countryid integer NOT NULL,
    country character(50),
    PRIMARY KEY (countryid)
);

CREATE TABLE public."softcartFactSales"
(
    "Orderid" integer NOT NULL,
    dateid integer,
    countryid integer,
    categoryid integer,
    amount integer,
    PRIMARY KEY ("Orderid")
);

ALTER TABLE public."softcartFactSales"
    ADD FOREIGN KEY (dateid)
    REFERENCES public."softcartDimDate" (dateid)
    NOT VALID;


ALTER TABLE public."softcartFactSales"
    ADD FOREIGN KEY (countryid)
    REFERENCES public."softcartDimCountry" (countryid)
    NOT VALID;


ALTER TABLE public."softcartFactSales"
    ADD FOREIGN KEY (categoryid)
    REFERENCES public."softcartDimCategory" (categoryid)
    NOT VALID;

END;