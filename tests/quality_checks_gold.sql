/*
===============================================================================
Quality Checks (Assertion Mode)
===============================================================================
Script Purpose:
    This script performs fail-fast quality checks to validate integrity and
    consistency in the Gold layer.

Behavior:
    - Each check raises an exception when violations are found.
    - Pipeline execution stops immediately on first failed check.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM gold.dim_customers
        GROUP BY customer_key
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'quality_checks_gold: duplicate customer_key in gold.dim_customers';
    END IF;
END $$;

-- ====================================================================
-- Checking 'gold.dim_products'
-- ====================================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM gold.dim_products
        GROUP BY product_key
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'quality_checks_gold: duplicate product_key in gold.dim_products';
    END IF;
END $$;

-- ====================================================================
-- Checking 'gold.dim_salesperson'
-- ====================================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM gold.dim_salesperson
        GROUP BY salesperson_key
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'quality_checks_gold: duplicate salesperson_key in gold.dim_salesperson';
    END IF;
END $$;

-- ====================================================================
-- Checking 'gold.dim_discount'
-- ====================================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM gold.dim_discount
        GROUP BY discount_key
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'quality_checks_gold: duplicate discount_key in gold.dim_discount';
    END IF;
END $$;

-- ====================================================================
-- Checking 'gold.fact_sales' -> core dimensions
-- ====================================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM gold.fact_sales f
        LEFT JOIN gold.dim_customers c
          ON c.customer_key = f.customer_key
        LEFT JOIN gold.dim_products p
          ON p.product_key = f.product_key
        WHERE c.customer_key IS NULL
           OR p.product_key IS NULL
    ) THEN
        RAISE EXCEPTION 'quality_checks_gold: orphan customer/product key in gold.fact_sales';
    END IF;
END $$;

