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
    FROM sales s
    JOIN companies c ON s.company_id = c.id
    LEFT JOIN subscriptions sub ON c.id = sub.company_id  
    LEFT JOIN plans p ON sub.plan_id = p.id  
    WHERE s.status IN ('TO_PRODUCE', 'PRODUCING', 'WAITING_PAYMENT', 'AVAILABLE_IN_INVENTORY')
    GROUP BY s.company_id, c.employees_quantity, p.name
),
Trimestres AS (
    SELECT generate_series(1, 12) AS relative_quarter
),
AllCombinations AS (
    SELECT 
        fs.company_id,
        fs.first_order_year AS cohort_year,
        fs.first_order_quarter AS cohort_quarter,
        fs.plan_name,
        fs.employee_range,
        t.relative_quarter
    FROM FirstSales fs
    CROSS JOIN Trimestres t
),
CompaniesInQuarter AS (
    SELECT 
        cohort_year,
        cohort_quarter,
        relative_quarter,
        plan_name,
        employee_range,
        COUNT(DISTINCT company_id) AS companies_in_quarter
    FROM AllCombinations
    GROUP BY cohort_year, cohort_quarter, relative_quarter, plan_name, employee_range
),
BaseCohortCompanies AS (
    SELECT 
        cohort_year,
        cohort_quarter,
        plan_name,
        employee_range,
        COUNT(DISTINCT company_id) AS base_companies_in_q1
    FROM AllCombinations
    WHERE relative_quarter = 1
    GROUP BY cohort_year, cohort_quarter, plan_name, employee_range
),
SalesWithQuarterAgg AS (
    SELECT 
        company_id,
        EXTRACT(YEAR FROM created_at) AS year,
        CEIL(EXTRACT(MONTH FROM created_at) / 3.0) AS quarter,
        COUNT(DISTINCT id) AS orders,
        SUM(total_price) AS total_price,
        COUNT(*) FILTER (WHERE status IN ('AVAILABLE_IN_INVENTORY', 'DELIVERED')) AS vendas_validas
    FROM sales
    WHERE status IN ('TO_PRODUCE', 'PRODUCING', 'WAITING_PAYMENT', 'AVAILABLE_IN_INVENTORY')
    GROUP BY company_id, year, quarter
),
DeliveredSalesAgg AS (
    SELECT 
        s.company_id,
        EXTRACT(YEAR FROM v.created_at) AS year,
        CEIL(EXTRACT(MONTH FROM v.created_at) / 3.0) AS quarter
    FROM versions v
    JOIN sale_items si ON si.id = v.item_id::int
    join sales s on si.sale_id = s.id
    WHERE v.item_type = 'SaleItem'
      AND COALESCE(v.object_changes->'status'->>1, v.object->>'status') = 'DELIVERED'
    GROUP BY s.company_id, year, quarter
),
OrdersRevenue AS (
    SELECT 
        ac.cohort_year,
        ac.cohort_quarter,
        ac.relative_quarter,
        ac.plan_name,
        ac.employee_range,

        COUNT(DISTINCT swq.company_id) AS active_companies_in_quarter,
        COALESCE(SUM(swq.orders), 0) AS orders_in_quarter,
        COALESCE(SUM(swq.total_price), 0) AS revenue_in_quarter,
        COUNT(DISTINCT ds.company_id) AS retencao_envio,
        COUNT(DISTINCT CASE WHEN swq.orders > 0 THEN swq.company_id END) AS retencao_venda

    FROM AllCombinations ac
    LEFT JOIN SalesWithQuarterAgg swq
        ON swq.company_id = ac.company_id
       AND ((swq.year * 4 + swq.quarter) - (ac.cohort_year * 4 + ac.cohort_quarter)) = ac.relative_quarter - 1
    LEFT JOIN DeliveredSalesAgg ds
        ON ds.company_id = ac.company_id
       AND ((ds.year * 4 + ds.quarter) - (ac.cohort_year * 4 + ac.cohort_quarter)) = ac.relative_quarter - 1

    GROUP BY ac.cohort_year, ac.cohort_quarter, ac.relative_quarter, ac.plan_name, ac.employee_range
),

RevenueQ1 AS (
    SELECT 
        fs.first_order_year AS cohort_year,
        fs.first_order_quarter AS cohort_quarter,
        fs.plan_name,
        fs.employee_range,
        SUM(s.total_price) AS revenue_q1
    FROM FirstSales fs
    JOIN sales s ON s.company_id = fs.company_id
    WHERE 
        s.status IN ('TO_PRODUCE', 'PRODUCING', 'WAITING_PAYMENT', 'AVAILABLE_IN_INVENTORY')
        AND EXTRACT(YEAR FROM s.created_at) = fs.first_order_year
        AND CEIL(EXTRACT(MONTH FROM s.created_at) / 3.0) = fs.first_order_quarter
    GROUP BY fs.first_order_year, fs.first_order_quarter, fs.plan_name, fs.employee_range
),
Final AS (
    SELECT 
        o.*,
        ciq.companies_in_quarter,
        rq.revenue_q1,
        SUM(orders_in_quarter) OVER (
            PARTITION BY o.cohort_year, o.cohort_quarter, o.plan_name, o.employee_range
            ORDER BY o.relative_quarter
        ) AS cumulative_orders,
        SUM(revenue_in_quarter) OVER (
            PARTITION BY o.cohort_year, o.cohort_quarter, o.plan_name, o.employee_range
            ORDER BY o.relative_quarter
        ) AS cumulative_revenue,
bq1.base_companies_in_q1
    FROM OrdersRevenue o
    LEFT JOIN RevenueQ1 rq
      ON rq.cohort_year = o.cohort_year
     AND rq.cohort_quarter = o.cohort_quarter
     AND rq.plan_name = o.plan_name
     AND rq.employee_range = o.employee_range
    LEFT JOIN CompaniesInQuarter ciq
      ON ciq.cohort_year = o.cohort_year
     AND ciq.cohort_quarter = o.cohort_quarter
     AND ciq.relative_quarter = o.relative_quarter
     AND ciq.plan_name = o.plan_name
     AND ciq.employee_range = o.employee_range
LEFT JOIN BaseCohortCompanies bq1
  ON bq1.cohort_year = o.cohort_year
 AND bq1.cohort_quarter = o.cohort_quarter
 AND bq1.plan_name = o.plan_name
 AND bq1.employee_range = o.employee_range

),

Filtered AS (
    SELECT * FROM Final
    WHERE revenue_q1 > 0
)
SELECT 
    (cohort_year || ' Q' || cohort_quarter) AS cohort,
    relative_quarter,
    plan_name,
    employee_range,
    companies_in_quarter, 
    active_companies_in_quarter, 
    orders_in_quarter, 
    revenue_in_quarter, 
    cumulative_orders,
    cumulative_revenue,
    retencao_envio,
    retencao_venda,
    revenue_q1, 
    base_companies_in_q1
FROM Filtered f
WHERE (f.cohort_year * 4 + f.cohort_quarter - 1 + relative_quarter) <
      (EXTRACT(YEAR FROM CURRENT_DATE) * 4 + CEIL(EXTRACT(MONTH FROM CURRENT_DATE)/3.0))
ORDER BY f.cohort_year, f.cohort_quarter, plan_name, employee_range, relative_quarter; 
