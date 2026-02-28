# Data Catalog – Gold Layer

## Overview

The **Gold Layer** represents the finalized, business-ready data model optimized for analytics and reporting. It follows a **star schema design**, consisting of dimension tables (descriptive attributes) and fact tables (measurable business events).

---

## 1. gold.dim_customers

**Purpose:**  
Contains enriched customer information combining CRM and ERP data to provide demographic and geographic attributes for business analysis.

| Column Name      | Data Type     | Description |
|------------------|--------------|-------------|
| customer_key     | INT          | Surrogate key generated for each customer record. |
| customer_id      | INT          | Unique numeric identifier from the source system. |
| customer_number  | NVARCHAR(50) | Business reference code used for tracking customers. |
| first_name       | NVARCHAR(50) | Customer’s given name. |
| last_name        | NVARCHAR(50) | Customer’s family name. |
| country          | NVARCHAR(50) | Country of residence. |
| marital_status   | NVARCHAR(50) | Customer’s marital status (e.g., Single, Married). |
| gender           | NVARCHAR(50) | Gender derived primarily from CRM, with ERP fallback if unavailable. |
| birthdate        | DATE         | Customer’s date of birth (YYYY-MM-DD). |
| create_date      | DATE         | Date when the customer record was created. |

---

## 2. gold.dim_products

**Purpose:**  
Stores product-related attributes used for sales performance and product-level analysis.

| Column Name     | Data Type     | Description |
|-----------------|--------------|-------------|
| product_key     | INT          | Surrogate key uniquely identifying each product record. |
| product_id      | INT          | Internal system identifier of the product. |
| product_number  | NVARCHAR(50) | Alphanumeric product reference code. |
| product_name    | NVARCHAR(50) | Descriptive name of the product. |
| category_id     | NVARCHAR(50) | Identifier linking the product to its category. |
| category        | NVARCHAR(50) | High-level classification (e.g., Bikes, Components). |
| subcategory     | NVARCHAR(50) | Detailed classification within the category. |
| maintenance     | NVARCHAR(50) | Indicates whether maintenance is required (Yes/No). |
| cost            | INT          | Base cost of the product in monetary units. |
| product_line    | NVARCHAR(50) | Product line or series (e.g., Road, Mountain). |
| start_date      | DATE         | Date when the product became active or available. |

---

## 3. gold.dim_salesperson

**Purpose:**  
Contains salesperson attributes for analyzing sales by representative and region.

| Column Name      | Data Type     | Description |
|------------------|--------------|-------------|
| salesperson_key  | INT          | Surrogate key for each salesperson. |
| salesperson_id   | INT          | Unique identifier from the source system. |
| salesperson_name | NVARCHAR(100)| Full name of the salesperson. |
| region           | NVARCHAR(50) | Assigned sales region. |
| email            | NVARCHAR(100)| Official email address. |

---

## 4. gold.dim_discount

**Purpose:**  
Stores discount information associated with sales transactions.

| Column Name          | Data Type     | Description |
|----------------------|--------------|-------------|
| discount_key         | INT          | Surrogate key for each discount record. |
| discount_id          | INT          | Unique identifier of the discount. |
| discount_description | NVARCHAR(100)| Description of the discount type or campaign. |
| discount_percent     | INT          | Discount percentage applied to the sale. |
| discount_active      | BOOLEAN      | Indicates whether the discount is currently active. |

---

## 5. gold.fact_sales

**Purpose:**  
Captures transactional sales data and links to related dimensions for analytical reporting.

| Column Name     | Data Type     | Description |
|-----------------|--------------|-------------|
| order_number    | NVARCHAR(50) | Unique sales order identifier. |
| product_key     | INT          | Foreign key referencing dim_products. |
| customer_key    | INT          | Foreign key referencing dim_customers. |
| order_date      | DATE         | Date the order was placed. |
| shipping_date   | DATE         | Date the order was shipped. |
| due_date        | DATE         | Payment due date. |
| sales_amount    | INT          | Total monetary value of the sale line. |
| quantity        | INT          | Number of units sold. |
| price           | INT          | Price per unit at the time of sale. |
| salesperson_key | INT          | Foreign key referencing dim_salesperson. |
| discount_key    | INT          | Foreign key referencing dim_discount. |
