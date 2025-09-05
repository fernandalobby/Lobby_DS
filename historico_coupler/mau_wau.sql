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
        MIN(s.created_at) AS first_order_date
    FROM 
        sales s
    JOIN companies c ON s.company_id = c.id
    LEFT JOIN subscriptions sub ON c.id = sub.company_id  
    LEFT JOIN plans p ON sub.plan_id = p.id  
    WHERE 
        s.status IN ('TO_PRODUCE', 'PRODUCING', 'WAITING_PAYMENT', 'AVAILABLE_IN_INVENTORY')
    GROUP BY 
        s.company_id, c.employees_quantity, p.name
),

VersoesComSemana AS (
    SELECT 
        v.*,
        -- Cálculo do primeiro dia da semana (segunda-feira)
        (v.created_at - ((EXTRACT(DOW FROM v.created_at)::int + 6) % 7) * INTERVAL '1 day')::date AS semana_data
    FROM versions v
)

SELECT
    c.name AS empresa,
    u.email AS usuario,

    -- Mês (baseado na data do evento)
    DATE_TRUNC('month', v.created_at)::date AS mes_data,
    TO_CHAR(DATE_TRUNC('month', v.created_at), 'MM/YYYY') AS mes_formatado,

    -- Semana (com base no primeiro dia da semana)
    v.semana_data,
    'Semana ' || TO_CHAR(v.semana_data, 'IW') || ' - ' || TO_CHAR(v.semana_data, 'DD/MM/YYYY') AS semana_formatada,

    -- Plano e faixa de funcionários
    COALESCE(fs.plan_name, 'Sem Plano') AS plano,
    fs.employee_range,

    -- Acessos totais
    COUNT(*) AS quantidade_de_acessos,

    -- Acessos na semana (de segunda a domingo)
    COUNT(*) FILTER (
        WHERE v.created_at >= v.semana_data
          AND v.created_at <  v.semana_data + INTERVAL '7 days'
    ) AS acessos_na_semana

FROM VersoesComSemana v
JOIN users u ON u.id::varchar = v.item_id
JOIN companies c ON c.id = u.company_id
LEFT JOIN FirstSales fs ON fs.company_id = c.id

WHERE 
    v.item_type = 'User'
    AND v.object_changes->'signin_token'->>0 IS NOT NULL
    AND v.object_changes->'signin_token'->>-1 IS NULL
    AND v.semana_data >= DATE '2025-05-01'

GROUP BY 
    c.id, u.id,
    mes_data, mes_formatado,
    v.semana_data, semana_formatada,
    fs.plan_name, fs.employee_range

ORDER BY v.semana_data, empresa, usuario;
