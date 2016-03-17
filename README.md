A mini SQL engine which runs a subset of SQL QUeries using command line interface. Python implementation.

List of Queries:
	1. Select all records: SELECT 8 FROM table_name

	2. Aggragate functions (Sum, average, max, min) : SELECT max(col1) FROM table1

	3. Project columns: SELECT col1,col2 FROM table_name

	4. Project with distinct from one table: SELECT distict(col1) FROM table_name

	5. Create tables: CREATE TABLE table_name(col1 datatype, col2 datatype)

	6. Insert record into a table: INSERT INTO table_name VALUES (v1,v2..vn)

	7. Delete a single record from a table: DELETE FROM table-name WHERE attribute = some_value

	8. Truncate table (delete all recods from a table): TRUNCATE TABLE table_name

	9. Drop Table (delete an empty table): DROP TABLE table_name

Data Format:
	1. .csv file for tables. File name table1.csv, table name is "table1" 

	2. All elements in file are only integers

	3. metadata.txt file: having the structure for each table

	    eg. <begin_table> 
		<table_name> 
		<attribute1> 
		.... 
 
		<attributeN> 
		<end_table>  
