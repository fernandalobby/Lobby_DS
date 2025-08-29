WITH FirstSales AS (
    SELECT 
        s.company_id,
        c.employees_quantity,
        COALESCE(p.name, 'Sem Plano') AS plan_name, 
        CASE 
            WHEN c.employees_quantity < 50 THEN 'Menor que cinquenta'
            WHEN c.employees_quantity BETWEEN 50 AND 300 THEN 'Entre cinquenta e trezentos'
            WHEN c.employees_quantity BETWEEN 301 AND 999 THEN 'Entre trezentos e mil'
            ELSE 'Maior que mil'
        END AS employee_range,
        MIN(s.created_at) AS first_order_date,
        EXTRACT(YEAR FROM MIN(s.created_at)) AS first_order_year,
        CEIL(EXTRACT(MONTH FROM MIN(s.created_at)) / 3.0) AS first_order_quarter
    FROM 
        sales s
    JOIN 
        companies c ON s.company_id = c.id
    LEFT JOIN 
        subscriptions sub ON c.id = sub.company_id  
    LEFT JOIN 
        plans p ON sub.plan_id = p.id  
    WHERE 
        s.status IN ('TO_PRODUCE', 'PRODUCING', 'WAITING_PAYMENT', 'AVAILABLE_IN_INVENTORY')
    GROUP BY 
        s.company_id, c.employees_quantity, p.name
),
Trimestres AS (
    SELECT generate_series(1, 12) AS relative_quarter
),
AllCombinations AS (
    SELECT 
        DISTINCT fs.first_order_year AS cohort_year,
        fs.first_order_quarter AS cohort_quarter,
        fs.plan_name,
        fs.employee_range,
        t.relative_quarter
    FROM FirstSales fs
    CROSS JOIN Trimestres t
),
DeliveredSales AS (
    SELECT DISTINCT
        s.id AS sale_id,
        s.company_id,
        DATE_TRUNC('month', v.created_at) AS delivered_month,
        EXTRACT(YEAR FROM v.created_at) AS year,
        CEIL(EXTRACT(MONTH FROM v.created_at) / 3.0) AS quarter
    FROM versions v
    JOIN sale_items si ON si.id = v.item_id::int
    JOIN sales s ON s.id = (v.object->>'sale_id')::uuid
    WHERE v.item_type = 'SaleItem'
      AND COALESCE(v.object_changes->'status'->>1, v.object->>'status') = 'DELIVERED'
),
OrdersRevenue AS (
    SELECT 
        ac.cohort_year,
        ac.cohort_quarter,
        ac.relative_quarter,
        ac.plan_name,
        ac.employee_range,
        COALESCE(SUM(DISTINCT s.total_price), 0) AS revenue_in_quarter,
        COALESCE(COUNT(DISTINCT s.id), 0) AS orders_in_quarter,
        COUNT(DISTINCT fs.company_id) AS companies_in_quarter,
        COALESCE(COUNT(DISTINCT s.company_id), 0) AS active_companies_in_quarter,
        COALESCE(COUNT(DISTINCT ds.company_id), 0) AS retencao_envio,
        COALESCE(COUNT(DISTINCT CASE 
            WHEN s.status IN ('AVAILABLE_IN_INVENTORY', 'DELIVERED') THEN s.company_id 
        END), 0) AS retencao_venda
    FROM 
        AllCombinations ac
    LEFT JOIN FirstSales fs 
        ON ac.cohort_year = fs.first_order_year 
       AND ac.cohort_quarter = fs.first_order_quarter
       AND ac.plan_name = fs.plan_name
       AND ac.employee_range = fs.employee_range
    LEFT JOIN (
        SELECT DISTINCT id, company_id, created_at, total_price, status
        FROM sales
    ) s
        ON s.company_id = fs.company_id
        AND ((EXTRACT(YEAR FROM s.created_at) - fs.first_order_year) * 4 +
            (CEIL(EXTRACT(MONTH FROM s.created_at) / 3.0) - fs.first_order_quarter) + 1) = ac.relative_quarter
        AND s.status IN ('TO_PRODUCE', 'PRODUCING', 'WAITING_PAYMENT', 'AVAILABLE_IN_INVENTORY')
    LEFT JOIN DeliveredSales ds
        ON ds.company_id = fs.company_id
        AND ((ds.year - fs.first_order_year) * 4 + (ds.quarter - fs.first_order_quarter) + 1) = ac.relative_quarter
    GROUP BY 
        ac.cohort_year, ac.cohort_quarter, ac.relative_quarter,
        ac.plan_name, ac.employee_range
),
RevenueQ1ByCohort AS (
    SELECT 
        cohort_year,
        cohort_quarter,
        plan_name,
        employee_range,
        revenue_in_quarter AS total_revenue_q1
    FROM OrdersRevenue
    WHERE relative_quarter = 1
)

SELECT 
    o.cohort_year || ' Q' || o.cohort_quarter AS cohort,
    o.relative_quarter,
    o.plan_name,
    o.employee_range,
    o.companies_in_quarter, 
    o.active_companies_in_quarter, 
    o.orders_in_quarter, 
    o.revenue_in_quarter, 
    SUM(o.orders_in_quarter) OVER (
        PARTITION BY o.cohort_year, o.cohort_quarter, o.plan_name, o.employee_range
        ORDER BY o.relative_quarter
    ) AS cumulative_orders,
    SUM(o.revenue_in_quarter) OVER (
        PARTITION BY o.cohort_year, o.cohort_quarter, o.plan_name, o.employee_range
        ORDER BY o.relative_quarter
    ) AS cumulative_revenue,
    o.retencao_envio,
    o.retencao_venda,
    ROUND(100.0 * o.retencao_envio::decimal / NULLIF(o.companies_in_quarter, 0), 2) AS retencao_envio_percentual,
    ROUND(100.0 * o.retencao_venda::decimal / NULLIF(o.companies_in_quarter, 0), 2) AS retencao_venda_percentual,
    ROUND(100.0 * o.revenue_in_quarter / NULLIF(rq1.total_revenue_q1, 0), 2) AS percentual_receita_vs_q1,
    (o.cohort_year + FLOOR((o.cohort_quarter - 1 + o.relative_quarter - 1) / 4)) || ' Q' || 
    ((MOD((o.cohort_quarter - 1 + o.relative_quarter - 1), 4) + 1)) AS real_quarter
FROM 
    OrdersRevenue o
JOIN RevenueQ1ByCohort rq1
  ON o.cohort_year = rq1.cohort_year
 AND o.cohort_quarter = rq1.cohort_quarter
 AND o.plan_name = rq1.plan_name
 AND o.employee_range = rq1.employee_range
WHERE (o.cohort_year * 4 + o.cohort_quarter - 1 + o.relative_quarter) <
      (EXTRACT(YEAR FROM CURRENT_DATE) * 4 + CEIL(EXTRACT(MONTH FROM CURRENT_DATE)/3.0))
      AND o.relative_quarter >= 1

ORDER BY 
    o.cohort_year, o.cohort_quarter, o.plan_name, o.employee_range, o.relative_quarter;
