# Customer Orders ETL Pipeline

## 📌 Project Overview
This project demonstrates a simple ETL (Extract, Transform, Load) data pipeline built using Python.

The pipeline reads customer order data from a CSV file, performs data cleaning and transformation, and generates a summary report.

---

## 🛠️ Technologies Used
- Python
- Pandas
- CSV Data Processing
- GitHub for Version Control

---

## 📂 Project Structure

customer-orders-pipeline/
│── data/
│   └── orders.csv
│── src/
│   └── pipeline.py
│── README.md

---

## ⚙️ ETL Process

### Extract
Reads raw customer order data from CSV file.

### Transform
- Removes missing values
- Calculates total order amount
- Aggregates revenue per customer

### Load
Outputs cleaned summary dataset.

---

## 📊 Sample Output

CustomerID | TotalRevenue
-----------|-------------
101        | 850
102        | 140
103        | 300

---

## 🎯 Learning Outcomes
- Built an end-to-end ETL pipeline
- Practiced data cleaning and transformation
- Learned project structuring for data engineering
- Used GitHub for version control

---

## 👩‍💻 Author
Srilekha Gaddam
