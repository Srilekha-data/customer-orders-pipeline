import pandas as pd

print("Customer Orders ETL Pipeline Started...")

# Extract
data = pd.read_csv("data/orders.csv")

print("Raw Data:")
print(data.head())

# Transform
data["TotalAmount"] = data["Quantity"] * data["Price"]

summary = data.groupby("CustomerID")["TotalAmount"].sum().reset_index()

print("Transformed Data:")
print(summary.head())

# Load
summary.to_csv("data/customer_summary.csv", index=False)

print("ETL Pipeline Completed Successfully!")
