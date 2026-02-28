/*
===============================================================================
Quality Checks (Assertion Mode)
===============================================================================
Script Purpose:
    This script performs fail-fast quality checks for data consistency, accuracy,
    and standardization across the Silver layer.

Behavior:
    - Each check raises an exception when violations are found.
    - Pipeline execution stops immediately on first failed check.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.crm_cust_info
        GROUP BY cst_id
        HAVING COUNT(*) > 1 OR cst_id IS NULL
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: duplicate or NULL cst_id in silver.crm_cust_info';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.crm_cust_info
        WHERE cst_key <> TRIM(cst_key)
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: unwanted spaces in silver.crm_cust_info.cst_key';
    END IF;
END $$;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.crm_prd_info
        GROUP BY prd_id
        HAVING COUNT(*) > 1 OR prd_id IS NULL
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: duplicate or NULL prd_id in silver.crm_prd_info';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.crm_prd_info
        WHERE prd_nm <> TRIM(prd_nm)
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: unwanted spaces in silver.crm_prd_info.prd_nm';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.crm_prd_info
        WHERE prd_cost < 0 OR prd_cost IS NULL
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: invalid prd_cost in silver.crm_prd_info';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.crm_prd_info
        WHERE prd_end_dt < prd_start_dt
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: prd_end_dt < prd_start_dt in silver.crm_prd_info';
    END IF;
END $$;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.crm_sales_details
        WHERE (sls_order_dt IS NOT NULL AND (sls_order_dt < DATE '1900-01-01' OR sls_order_dt > DATE '2050-01-01'))
           OR (sls_ship_dt IS NOT NULL AND (sls_ship_dt < DATE '1900-01-01' OR sls_ship_dt > DATE '2050-01-01'))
           OR (sls_due_dt IS NOT NULL AND (sls_due_dt < DATE '1900-01-01' OR sls_due_dt > DATE '2050-01-01'))
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: invalid or out-of-range dates in silver.crm_sales_details';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.crm_sales_details
        WHERE (sls_ship_dt IS NOT NULL AND sls_order_dt > sls_ship_dt)
           OR (sls_due_dt IS NOT NULL AND sls_order_dt > sls_due_dt)
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: invalid date order in silver.crm_sales_details';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.crm_sales_details
        WHERE sls_sales <> sls_quantity * sls_price
           OR sls_sales IS NULL
           OR sls_quantity IS NULL
           OR sls_price IS NULL
           OR sls_sales <= 0
           OR sls_quantity <= 0
           OR sls_price <= 0
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: sales consistency violation in silver.crm_sales_details';
    END IF;
END $$;

-- ====================================================================
-- Checking 'silver.erp_cust_info'
-- ====================================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.erp_cust_info
        WHERE bdate < DATE '1924-01-01'
           OR bdate > CURRENT_DATE
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: out-of-range bdate in silver.erp_cust_info';
    END IF;
END $$;

-- ====================================================================
-- Checking 'silver.erp_px_cat_info'
-- ====================================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.erp_px_cat_info
        WHERE cat <> TRIM(cat)
           OR subcat <> TRIM(subcat)
           OR maintenance <> TRIM(maintenance)
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: unwanted spaces in silver.erp_px_cat_info';
    END IF;
END $$;

-- ====================================================================
-- Checking 'silver.marketing_salesperson'
-- ====================================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.marketing_salesperson
        GROUP BY salesperson_id
        HAVING COUNT(*) > 1 OR salesperson_id IS NULL
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: duplicate or NULL salesperson_id in silver.marketing_salesperson';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.marketing_salesperson
        WHERE name <> TRIM(name)
           OR region <> TRIM(region)
           OR email <> TRIM(email)
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: unwanted spaces in silver.marketing_salesperson';
    END IF;
END $$;

-- ====================================================================
-- Checking 'silver.marketing_discount_info'
-- ====================================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.marketing_discount_info
        GROUP BY discount_id
        HAVING COUNT(*) > 1 OR discount_id IS NULL
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: duplicate or NULL discount_id in silver.marketing_discount_info';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.marketing_discount_info
        WHERE percent < 0 OR percent > 100 OR percent IS NULL
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: invalid percent in silver.marketing_discount_info';
    END IF;
END $$;

-- ====================================================================
-- Checking 'silver.marketing_salesperson_sales'
-- ====================================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.marketing_salesperson_sales mss
        LEFT JOIN silver.marketing_salesperson ms
          ON ms.salesperson_id = mss.salesperson_id
        WHERE ms.salesperson_id IS NULL
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: orphan salesperson_id in silver.marketing_salesperson_sales';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.marketing_salesperson_sales mss
        LEFT JOIN silver.crm_sales_details csd
          ON csd.sls_ord_num = mss.sls_ord_num
        WHERE csd.sls_ord_num IS NULL
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: orphan sls_ord_num in silver.marketing_salesperson_sales';
    END IF;
END $$;

-- ====================================================================
-- Checking 'silver.marketing_sales_discount'
-- ====================================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.marketing_sales_discount msd
        LEFT JOIN silver.marketing_discount_info mdi
          ON mdi.discount_id = msd.discount_id
        WHERE mdi.discount_id IS NULL
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: orphan discount_id in silver.marketing_sales_discount';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.marketing_sales_discount msd
        LEFT JOIN silver.crm_sales_details csd
          ON csd.sls_ord_num = msd.sls_ord_num
        WHERE csd.sls_ord_num IS NULL
    ) THEN
        RAISE EXCEPTION 'quality_checks_silver: orphan sls_ord_num in silver.marketing_sales_discount';
    END IF;
END $$;
