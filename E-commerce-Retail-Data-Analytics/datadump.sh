#!/bin/bash

# MySQL database connection details
database = sales
username = debabratagar
password = MzIzODctZGViYWJy

# Output file
output_file="sales_data.sql"

# mysqldump command to export data
mysqldump -u "$username" -p"$password" "$database" sales_data > "$output_file"

# Check if the mysqldump command was successful
if [ $? -eq 0 ]; then
    echo "Data export successful. File: $output_file"
else
    echo "Error exporting data. Check your credentials and database/table names."
fi