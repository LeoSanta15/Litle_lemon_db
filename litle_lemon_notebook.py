# Import MySQL Connector/Python 
import mysql.connector as connector
#Establish a connection
connection=connector.connect(user="root",password="")
# Create a cursor
cursor = connection.cursor()
#Create the database and set it for use
cursor.execute("CREATE DATABASE little_lemon_db") 
cursor.execute("USE little_lemon_db")

#Create tables
#MenuItems table
create_menuitem_table = """CREATE TABLE MenuItems (
ItemID INT AUTO_INCREMENT,
Name VARCHAR(200),
Type VARCHAR(100),
Price INT,
PRIMARY KEY (ItemID)
);"""

#Menu table
create_menu_table = """CREATE TABLE Menus (
MenuID INT,
ItemID INT,
Cuisine VARCHAR(100),
PRIMARY KEY (MenuID,ItemID)
);"""

#Bookings table
Create_booking_table = """CREATE TABLE Bookings (
BookingID INT AUTO_INCREMENT,
TableNo INT,
GuestFirstName VARCHAR(100) NOT NULL,
GuestLastName VARCHAR(100) NOT NULL,
BookingSlot TIME NOT NULL,
EmployeeID INT,
PRIMARY KEY (BookingID)
);"""

#Orders table
create_orders_table = """CREATE TABLE Orders (
OrderID INT,
TableNo INT,
MenuID INT,
BookingID INT,
BillAmount INT,
Quantity INT,
PRIMARY KEY (OrderID,TableNo)
);"""

#Employees table
create_employees_table = """CREATE TABLE Employees (
EmployeeID INT AUTO_INCREMENT PRIMARY KEY,
Name VARCHAR,
Role VARCHAR,
Address VARCHAR,
Contact_Number INT,
Email VARCHAR,
Annual_Salary VARCHAR
);"""

#Now that you have defined the structures for all the required tables, use the code below and create the tables in your database little_lemon_db.
# Create MenuItems table
cursor.execute(create_menuitem_table)

# Create Menu table
cursor.execute(create_menu_table)

# Create Bookings table
cursor.execute(Create_booking_table)

# Create Orders table
cursor.execute(create_orders_table)

# Create Employees table
cursor.execute(create_employees_table)

## Insert data
#*******************************************************#
# Insert query to populate "MenuItems" table:
#*******************************************************#
insert_menuitems="""
INSERT INTO MenuItems (ItemID, Name, Type, Price)
VALUES
(1, 'Olives','Starters',5),
(2, 'Flatbread','Starters', 5),
(3, 'Minestrone', 'Starters', 8),
(4, 'Tomato bread','Starters', 8),
(5, 'Falafel', 'Starters', 7),
(6, 'Hummus', 'Starters', 5),
(7, 'Greek salad', 'Main Courses', 15),
(8, 'Bean soup', 'Main Courses', 12),
(9, 'Pizza', 'Main Courses', 15),
(10, 'Greek yoghurt','Desserts', 7),
(11, 'Ice cream', 'Desserts', 6),
(12, 'Cheesecake', 'Desserts', 4),
(13, 'Athens White wine', 'Drinks', 25),
(14, 'Corfu Red Wine', 'Drinks', 30),
(15, 'Turkish Coffee', 'Drinks', 10),
(16, 'Turkish Coffee', 'Drinks', 10),
(17, 'Kabasa', 'Main Courses', 17);"""

#*******************************************************#
# Insert query to populate "Menu" table:
#*******************************************************#
insert_menu="""
INSERT INTO Menus (MenuID,ItemID,Cuisine)
VALUES
(1, 1, 'Greek'),
(1, 7, 'Greek'),
(1, 10, 'Greek'),
(1, 13, 'Greek'),
(2, 3, 'Italian'),
(2, 9, 'Italian'),
(2, 12, 'Italian'),
(2, 15, 'Italian'),
(3, 5, 'Turkish'),
(3, 17, 'Turkish'),
(3, 11, 'Turkish'),
(3, 16, 'Turkish');"""

#*******************************************************#
# Insert query to populate "Bookings" table:
#*******************************************************#
insert_bookings="""
INSERT INTO Bookings (BookingID, TableNo, GuestFirstName, 
GuestLastName, BookingSlot, EmployeeID)
VALUES
(1, 12, 'Anna','Iversen','19:00:00',1),
(2, 12, 'Joakim', 'Iversen', '19:00:00', 1),
(3, 19, 'Vanessa', 'McCarthy', '15:00:00', 3),
(4, 15, 'Marcos', 'Romero', '17:30:00', 4),
(5, 5, 'Hiroki', 'Yamane', '18:30:00', 2),
(6, 8, 'Diana', 'Pinto', '20:00:00', 5);"""

#*******************************************************#
# Insert query to populate "Orders" table:
#*******************************************************#
insert_orders="""
INSERT INTO Orders (OrderID, TableNo, MenuID, BookingID, Quantity, BillAmount)
VALUES
(1, 12, 1, 1, 2, 86),
(2, 19, 2, 2, 1, 37),
(3, 15, 2, 3, 1, 37),
(4, 5, 3, 4, 1, 40),
(5, 8, 1, 5, 1, 43);"""

#*******************************************************#
# Insert query to populate "Employees" table:
#*******************************************************#
insert_employees = """
INSERT INTO employees (EmployeeID, Name, Role, Address, Contact_Number, Email, Annual_Salary)
(01,'Mario Gollini','Manager','724, Parsley Lane, Old Town, Chicago, IL',351258074,'Mario.g@littlelemon.com','$70,000'),
(02,'Adrian Gollini','Assistant Manager','334, Dill Square, Lincoln Park, Chicago, IL',351474048,'Adrian.g@littlelemon.com','$65,000'),
(03,'Giorgos Dioudis','Head Chef','879 Sage Street, West Loop, Chicago, IL',351970582,'Giorgos.d@littlelemon.com','$50,000'),
(04,'Fatma Kaya','Assistant Chef','132  Bay Lane, Chicago, IL',351963569,'Fatma.k@littlelemon.com','$45,000'),
(05,'Elena Salvai','Head Waiter','989 Thyme Square, EdgeWater, Chicago, IL',351074198,'Elena.s@littlelemon.com','$40,000'),
(06,'John Millar','Receptionist','245 Dill Square, Lincoln Park, Chicago, IL',351584508,'John.m@littlelemon.com','$35,000');"""

#Populate the tables using execute module on the cursor. 
# Populate MenuItems table
cursor.execute(insert_menuitems)
connection.commit()

# Populate MenuItems table
cursor.execute(insert_menu)
connection.commit()

# Populate Bookings table
cursor.execute(insert_bookings)
connection.commit()

# Populate Orders table
cursor.execute(insert_orders)
connection.commit()

# Populate Employees table
cursor.execute(insert_employees)
connection.commit()

#*******************************************************#
# Task 1: Establish a connection
#*******************************************************#
# Step 1-2: Import the necessary modules
from mysql.connector import MySQLConnectionPool, Error

# Step 3: Create a pool named pool_a with two connections
dbconfig = {
        "database": "little_lemon_db",
        "user": "root",
        "password": ""
    }

try:
    pool_a = MySQLConnectionPool(pool_name="pool_a", pool_size=2, **dbconfig)
    print("Connection pool is created with the name", pool_a.pool_name)
    print("The pool size is created with a pool size", pool_a.pool_size)
except Error as e:
    print(f"Error while creating connection pool: {e}")

# Step four: Obtain a connection from pool_a and create a cursor object to communicate with the database.
try:
    # Obtain a connection from the connection pool
    connection = pool_a.get_connection()

    if connection.is_connected():
        print("Successfully connected to the database.")

        # Create a cursor object to communicate with the database
        cursor = connection.cursor()
        
except Error as e:
    print(f"Error while connecting to the database: {e}")
    

#*******************************************************#
# Task 2: Implement a stored procedure called PeakHours
#*******************************************************#

try:
    # Step one: Write a SQL CREATE PROCEDURE query for PeakHours
    create_procedure_query = """
    CREATE PROCEDURE PeakHours()
    BEGIN
        SELECT HOUR(BookingSlot) AS Hour, COUNT(*) AS NumBookings
        FROM Bookings
        GROUP BY Hour
        ORDER BY NumBookings DESC;
    END
    """

    # Execute the CREATE PROCEDURE query
    cursor.execute(create_procedure_query)
    print("Stored procedure 'PeakHours' created successfully.")

except Error as e:
    print(f"Error while creating the stored procedure: {e}")

# Step 2: Run the stored procedure query by invoking execute module on the cursor
try:
    # Run the stored procedure query
    cursor.execute("CALL PeakHours()")
    print("Stored procedure 'PeakHours' executed successfully.")

except Error as e:
    print(f"Error while executing the stored procedure: {e}")

# Step 3: Invoke callproc to call the stored procedure
try:
    # Call the stored procedure
    cursor.callproc("PeakHours")
    print("Stored procedure 'PeakHours' called successfully.")

except Error as e:
    print(f"Error while calling the stored procedure: {e}")

# Step 4: Fetch the results in a variable called dataset
# Fetch the results
dataset = cursor.fetchall()

# Step 5: Extract the names of the columns
# Extract the column names
column_names = [column[0] for column in cursor.description]

# Step 6: Print the names of the columns
# Print the column names
print("Column names:")
for name in column_names:
    print(name)
    
# Step 7: Print the sorted data using a for loop
# Print the sorted data
print("Sorted data:")
for row in dataset:
    print(row)

#*******************************************************#
# Task 3: Implement a stored procedure called PeakHours
#*******************************************************#

# Steps 1-4
try:
    # Create the stored procedure query
    create_procedure_query = """
    CREATE PROCEDURE GuestStatus()
    BEGIN
        SELECT CONCAT(BookingFirstName, ' ', BookingLastName) AS GuestName,
            CASE
                WHEN Role = 'Manager' OR Role = 'Assistant Manager' THEN 'Ready to pay'
                WHEN Role = 'Head Chef' THEN 'Ready to serve'
                WHEN Role = 'Assistant Chef' THEN 'Preparing Order'
                WHEN Role = 'Head Waiter' THEN 'Order served'
                ELSE 'Unknown'
            END AS Status
        FROM Bookings
        LEFT JOIN Employees ON Bookings.EmployeeID = Employees.EmployeeID;
    END
    """

    # Execute the CREATE PROCEDURE query
    cursor.execute(create_procedure_query)
    print("Stored procedure 'GuestStatus' created successfully.")

except Error as e:
    print(f"Error while creating the stored procedure: {e}")

# Step 5: Run the stored procedure query by invoking the execute module on the cursor.
try:
    # Run the stored procedure query
    cursor.execute("CALL GuestStatus()")
    print("Stored procedure 'GuestStatus' executed successfully.")

except Error as e:
    print(f"Error while executing the stored procedure: {e}")

# Step 6: Invoke callproc to call the stored procedure.
try:
    # Call the stored procedure
    cursor.callproc("GuestStatus")
    print("Stored procedure 'GuestStatus' called successfully.")

except Error as e:
    print(f"Error while calling the stored procedure: {e}")

# Step 7: Fetch the results in a variable called dataset.
# Fetch the results
dataset = cursor.fetchall()

# Step 8: Extract the names of the columns.
# Extract the column names
column_names = [column[0] for column in cursor.description]


# Step 9: Print the names of the columns.
print("Column names:")
for name in column_names:
    print(name)

# Step 10: Print the sorted data using a for loop.
print("Sorted data:")
for row in dataset:
    print(row)

# Step 11: Close the connection to return it back to the pool.
cursor.close()
connection.close()
print("Connection and cursor closed.")
