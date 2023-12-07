#!/bin/sh

## Write your code here to load the data from sales_data table in Mysql server to a sales.csv.
## Select the data which is not more than 4 hours old from the current time.

# Output file
output_file="sales.csv"

# MySQL query to select data not more than 4 hours old
query="USE sales; SELECT * FROM sales_data WHERE timestamp >= NOW() - INTERVAL 4 HOUR;"

# mysql command to execute the query and export to CSV
mysql --host=127.0.0.1 --port=3306 --user=root --password=MTEwMzQtZGViYWJy -e "$query" | sed 's/\t/,/g' > "$output_file"

# Check if the mysql command was successful
if [ $? -eq 0 ]; then
    echo "Data export successful. File: $output_file"
else
    echo "Error exporting data. Check your credentials and database/table names."
fi

 export PGPASSWORD=NzY4MS1kZWJhYnJh;



 psql --username=postgres --host=localhost --dbname=sales_new -c "\COPY sales_data(rowid,product_id,customer_id,price,quantity,timestamp) FROM '/home/project/sales.csv' delimiter ',' csv header;" 

 ## Delete sales.csv present in location /home/project

 file_path="/home/project/sales.csv"

# Check if the file exists before attempting to delete
if [ -e "$file_path" ]; then
    rm "$file_path"
    echo "File $file_path deleted successfully."
else
    echo "File $file_path does not exist."
fi

 ## Write your code here to load the DimDate table from the data present in sales_data table

 psql --username=postgres --host=localhost --dbname=sales_new -c  "INSERT INTO dimdate (dateid, day, month, year)
       SELECT DISTINCT dateid,
                       EXTRACT(DAY FROM timestamp) AS day,
                       EXTRACT(MONTH FROM timestamp) AS month,
                       EXTRACT(YEAR FROM timestamp) AS year
       FROM sales_data;"

# Check if the psql command was successful
if [ $? -eq 0 ]; then
    echo "Data loaded successfully into DimDate table."
else
    echo "Error loading data. Check your credentials and database/table names."
fi

## Write your code here to load the FactSales table from the data present in sales_data table

psql --username=postgres --host=localhost --dbname=sales_new -c  "INSERT INTO factsales (rowid, product_id, customer_id, price, total_price)
       SELECT rowid,
              product_id,
              customer_id,
              price,
              price * quantity AS total_price
       FROM sales_data;"

# Check if the psql command was successful
if [ $? -eq 0 ]; then
    echo "Data loaded successfully into FactSales table."
else
    echo "Error loading data. Check your credentials and database/table names."
fi

 ## Write your code here to export DimDate table to a csv

 psql --username=postgres --host=localhost --dbname=sales_new -c "SELECT * FROM dimdate" | sed 's/\t/,/g' > "DimDate.csv"

 ## Write your code here to export FactSales table to a csv
 
 psql --username=postgres --host=localhost --dbname=sales_new -c "SELECT * FROM factsales" | sed 's/\t/,/g' > "FactSales.csv"