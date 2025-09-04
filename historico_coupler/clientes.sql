WITH FirstSales AS (
    SELECT 
        s.company_id,
        c.name AS company_name,
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
    GROUP BY s.company_id, c.name, c.employees_quantity, p.name
),
RealSales AS (
    SELECT 
        s.company_id,
        EXTRACT(YEAR FROM s.created_at) AS real_year,
        CEIL(EXTRACT(MONTH FROM s.created_at) / 3.0) AS real_quarter
    FROM sales s
    WHERE s.status IN ('TO_PRODUCE', 'PRODUCING', 'WAITING_PAYMENT', 'AVAILABLE_IN_INVENTORY')
)

SELECT 
    fs.company_name,
    fs.plan_name,
    fs.employee_range,
    fs.first_order_year || ' Q' || fs.first_order_quarter AS cohort,
    rs.real_year || ' Q' || rs.real_quarter AS real_quarter
FROM FirstSales fs
JOIN RealSales rs ON fs.company_id = rs.company_id
ORDER BY cohort, real_quarter, company_name;
