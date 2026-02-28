# Naming Conventions

This document defines the standard naming conventions applied to schemas, tables, views, columns, and other data warehouse objects.

---

## Table of Contents

1. [General Principles](#general-principles)
2. [Table Naming Standards](#table-naming-standards)
   - [Bronze Layer](#bronze-layer)
   - [Silver Layer](#silver-layer)
   - [Gold Layer](#gold-layer)
3. [Column Naming Standards](#column-naming-standards)
   - [Surrogate Keys](#surrogate-keys)
   - [Technical Columns](#technical-columns)
4. [Stored Procedure Naming](#stored-procedure-naming)

---

## General Principles

- **Format**: Use `snake_case` (all lowercase letters with underscores between words).
- **Language**: All object names must be written in English.
- **Reserved Words**: Avoid using SQL reserved keywords as names.

---

## Table Naming Standards

### Bronze Layer

- Table names must begin with the source system name.
- Table names must remain identical to their original source names (no renaming allowed).

**Pattern:**  
`<sourcesystem>_<entity>`

- `<sourcesystem>`: The originating system (e.g., `crm`, `erp`, `marketing`).
- `<entity>`: The exact table name from the source system.

**Example:**  
`crm_customer_info` → Customer information extracted from the CRM system.

---

### Silver Layer

- Table names must also start with the source system name.
- The original table names must be preserved without modification.

**Pattern:**  
`<sourcesystem>_<entity>`

- `<sourcesystem>`: Name of the source system.
- `<entity>`: Original table name from the source.

**Example:**  
`crm_customer_info`

---

### Gold Layer

- Table names must use clear, business-focused naming.
- Each table must begin with a category prefix that reflects its role.

**Pattern:**  
`<category>_<entity>`

- `<category>`: Indicates table type (e.g., `dim`, `fact`).
- `<entity>`: Business-aligned descriptive name (e.g., `customers`, `products`, `sales`).

**Examples:**

- `dim_customers` → Customer dimension table.
- `fact_sales` → Sales transaction fact table.

---

### Category Prefix Reference

| Prefix   | Description        | Example                    |
|-----------|-------------------|----------------------------|
| `dim_`    | Dimension table   | `dim_customer`, `dim_product` |
| `fact_`   | Fact table        | `fact_sales`               |
| `report_` | Reporting table   | `report_sales_monthly`     |

---

## Column Naming Standards

### Surrogate Keys

- All dimension table primary keys must end with `_key`.

**Pattern:**  
`<entity>_key`

- `_key`: Identifies a surrogate key column.

**Example:**  
`customer_key` → Surrogate key in `dim_customers`.

---

### Technical Columns

- System-generated metadata columns must begin with the prefix `dwh_`.

**Pattern:**  
`dwh_<column_name>`

- `dwh`: Indicates data warehouse technical metadata.
- `<column_name>`: Clearly describes the column’s function.

**Example:**  
`dwh_load_date` → Date when the record was loaded into the warehouse.

---

## Stored Procedure Naming

- Stored procedures used for data loading must follow this pattern:

**Pattern:**  
`load_<layer>`

- `<layer>`: The target layer (e.g., `bronze`, `silver`, `gold`).

**Examples:**

- `load_bronze` → Loads data into the Bronze layer.
- `load_silver` → Loads data into the Silver layer.
- `load_gold` → Loads data into the Gold layer.
