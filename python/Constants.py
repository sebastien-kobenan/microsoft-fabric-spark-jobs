
fileFormat = "csv"
folderPath = "Files/adventureworks"

calendarSource = f"{folderPath}/AdventureWorks_Calendar.csv"
calendarTable = "dimcalendar"

categorySource = f"{folderPath}/AdventureWorks_Product_Categories.csv"
subcategorySource = f"{folderPath}/AdventureWorks_Product_Subcategories.csv"
productSource = f"{folderPath}/AdventureWorks_Products.csv"
productTable = "dimproducts"

customerSource = f"{folderPath}/AdventureWorks_Customers.csv"
customerTable = "dimcustomers"

saleSource = f"{folderPath}/AdventureWorks_Sales_*"
saleTable = "factsales"

