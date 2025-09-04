WITH unpacked_events AS (
    SELECT
        v.item_id::int AS sale_item_id,
        (v.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS event_created_at_sao_paulo,
        v.created_at AS event_created_at_utc,
        COALESCE(
            v.object_changes->'status'->>1,
            v.object->>'status'
        ) AS event_type,
        (v.object->>'sale_id')::uuid AS sale_id
    FROM versions v
    WHERE v.item_type = 'SaleItem'
      AND v.created_at::date > '2024-01-01'
),
ordered_events AS (
    SELECT 
        sale_item_id,
        sale_id,
        '''' || TO_CHAR(event_created_at_sao_paulo, 'YYYY-MM') AS event_month_year,
        event_type AS status,
        event_created_at_sao_paulo,
        event_created_at_utc,
        ROW_NUMBER() OVER (PARTITION BY sale_item_id ORDER BY event_created_at_sao_paulo, event_type) AS rn
    FROM unpacked_events
    WHERE event_type IS NOT NULL AND event_type <> ''
),
feriados AS (
    SELECT unnest(ARRAY[
        '2024-01-01'::DATE, '2024-02-12', '2024-02-13', '2024-03-29', '2024-04-21', '2024-05-01',
        '2024-05-30', '2024-09-07', '2024-10-12', '2024-11-02', '2024-11-15', '2024-12-25',
        '2025-01-01', '2025-03-03', '2025-03-04', '2025-04-18', '2025-04-21', '2025-05-01',
        '2025-06-19', '2025-09-07', '2025-10-12', '2025-11-02', '2025-11-15', '2025-12-25'
    ]) AS holiday_date
),
status_pairs AS (
    SELECT 
        e1.sale_item_id,
        e1.sale_id,
        e1.event_month_year,
        e1.status AS previous_status,
        e2.status AS current_status,
        e1.event_created_at_sao_paulo AS previous_created_at,
        e2.event_created_at_sao_paulo AS current_created_at,
        
        CASE  
            WHEN e1.event_created_at_sao_paulo::time <= '18:00:00' AND DATE_TRUNC('day', e1.event_created_at_sao_paulo) = DATE_TRUNC('day', e2.event_created_at_sao_paulo) THEN 1
            WHEN e1.event_created_at_sao_paulo::time > '18:00:00' AND DATE_TRUNC('day', e1.event_created_at_sao_paulo) = DATE_TRUNC('day', e2.event_created_at_sao_paulo) THEN 2
            WHEN e1.event_created_at_sao_paulo::time > '18:00:00' AND DATE_TRUNC('day', e2.event_created_at_sao_paulo) = DATE_TRUNC('day', e1.event_created_at_sao_paulo + INTERVAL '1 day') THEN 3
            WHEN e1.event_created_at_sao_paulo::time <= '18:00:00' AND DATE_TRUNC('day', e2.event_created_at_sao_paulo) = DATE_TRUNC('day', e1.event_created_at_sao_paulo + INTERVAL '1 day') THEN 4
            WHEN e1.event_created_at_sao_paulo::time > '18:00:00' AND DATE_TRUNC('day', e2.event_created_at_sao_paulo) > DATE_TRUNC('day', e1.event_created_at_sao_paulo + INTERVAL '1 day') THEN 5
            WHEN e1.event_created_at_sao_paulo::time <= '18:00:00' AND DATE_TRUNC('day', e2.event_created_at_sao_paulo) > DATE_TRUNC('day', e1.event_created_at_sao_paulo + INTERVAL '1 day') THEN 6
        END AS hours_between_status_teste,

        CASE
            WHEN e1.event_created_at_sao_paulo::time <= '18:00:00' AND DATE_TRUNC('day', e1.event_created_at_sao_paulo) = DATE_TRUNC('day', e2.event_created_at_sao_paulo) THEN
                EXTRACT(EPOCH FROM (e2.event_created_at_sao_paulo - e1.event_created_at_sao_paulo)) / 3600
            WHEN e1.event_created_at_sao_paulo::time > '18:00:00' AND DATE_TRUNC('day', e1.event_created_at_sao_paulo) = DATE_TRUNC('day', e2.event_created_at_sao_paulo) THEN
                EXTRACT(EPOCH FROM (e2.event_created_at_sao_paulo - e1.event_created_at_sao_paulo)) / 3600
            WHEN e1.event_created_at_sao_paulo::time > '18:00:00' AND DATE_TRUNC('day', e2.event_created_at_sao_paulo) = DATE_TRUNC('day', e1.event_created_at_sao_paulo + INTERVAL '1 day') THEN
                EXTRACT(EPOCH FROM (e2.event_created_at_sao_paulo - (DATE_TRUNC('day', e1.event_created_at_sao_paulo) + INTERVAL '1 day' + INTERVAL '9 hours'))) / 3600
            WHEN e1.event_created_at_sao_paulo::time <= '18:00:00' AND DATE_TRUNC('day', e2.event_created_at_sao_paulo) = DATE_TRUNC('day', e1.event_created_at_sao_paulo + INTERVAL '1 day') THEN
                (EXTRACT(EPOCH FROM (e2.event_created_at_sao_paulo - (DATE_TRUNC('day', e1.event_created_at_sao_paulo) + INTERVAL '1 day' + INTERVAL '9 hours'))) / 3600) +
                (EXTRACT(EPOCH FROM ((DATE_TRUNC('day', e1.event_created_at_sao_paulo) + INTERVAL '18 hours') - e1.event_created_at_sao_paulo)) / 3600)
            WHEN e1.event_created_at_sao_paulo::time > '18:00:00' AND DATE_TRUNC('day', e2.event_created_at_sao_paulo) > DATE_TRUNC('day', e1.event_created_at_sao_paulo + INTERVAL '1 day') THEN
                (EXTRACT(EPOCH FROM (e2.event_created_at_sao_paulo - (DATE_TRUNC('day', e2.event_created_at_sao_paulo) + INTERVAL '9 hours'))) / 3600) +
                (SELECT COUNT(*) 
                 FROM generate_series(DATE_TRUNC('day', e1.event_created_at_sao_paulo) + INTERVAL '1 day', 
                                      DATE_TRUNC('day', e2.event_created_at_sao_paulo) - INTERVAL '1 day', 
                                      INTERVAL '1 day') AS gs(day)
                 WHERE EXTRACT(DOW FROM gs.day) NOT IN (0, 6)
                   AND gs.day NOT IN (SELECT holiday_date FROM feriados)) * 9
            WHEN e1.event_created_at_sao_paulo::time <= '18:00:00' AND DATE_TRUNC('day', e2.event_created_at_sao_paulo) > DATE_TRUNC('day', e1.event_created_at_sao_paulo + INTERVAL '1 day') THEN
                (EXTRACT(EPOCH FROM (e2.event_created_at_sao_paulo - (DATE_TRUNC('day', e2.event_created_at_sao_paulo) + INTERVAL '9 hours'))) / 3600) +
                (EXTRACT(EPOCH FROM ((DATE_TRUNC('day', e1.event_created_at_sao_paulo) + INTERVAL '18 hours') - e1.event_created_at_sao_paulo)) / 3600) +
                (SELECT COUNT(*) 
                 FROM generate_series(DATE_TRUNC('day', e1.event_created_at_sao_paulo) + INTERVAL '1 day', 
                                      DATE_TRUNC('day', e2.event_created_at_sao_paulo) - INTERVAL '1 day', 
                                      INTERVAL '1 day') AS gs(day)
                 WHERE EXTRACT(DOW FROM gs.day) NOT IN (0, 6)
                   AND gs.day NOT IN (SELECT holiday_date FROM feriados)) * 9
        END AS hours_between_status
    FROM ordered_events e1
    LEFT JOIN ordered_events e2 
        ON e1.sale_item_id = e2.sale_item_id
        AND e1.rn = e2.rn - 1
    WHERE e1.status IS NOT NULL 
      AND e2.status IS NOT NULL
      AND e2.event_created_at_sao_paulo >= e1.event_created_at_sao_paulo
),
filtered_status_pairs AS (
    SELECT *,
        LAG(previous_status) OVER (PARTITION BY sale_item_id ORDER BY previous_created_at) AS last_previous_status,
        LAG(current_status) OVER (PARTITION BY sale_item_id ORDER BY current_created_at) AS last_current_status
    FROM status_pairs
),
sale_item_details AS (
    SELECT 
        si.id AS sale_item_id,
        si.product_id,
        si.customer_product_id
    FROM sale_items si
),
product_info AS (
    SELECT 
        si.id AS sale_item_id,
        COALESCE(p.sku, p2.sku) AS sku,
        COALESCE(p.name, p2.name) AS product_name,
        COALESCE(pc.name, pc2.name) AS categoria_produto,
        COALESCE(p.id, p2.id) AS product_id
    FROM sale_items si
    LEFT JOIN customer_products cp ON si.customer_product_id = cp.id
    LEFT JOIN products p ON cp.product_id = p.id
    LEFT JOIN product_categories pc ON pc.id = p.category_id
    LEFT JOIN products p2 ON si.product_id = p2.id
    LEFT JOIN product_categories pc2 ON pc2.id = p2.category_id
),
sale_info AS (
    SELECT 
        s.id AS sale_id,
        s.code AS sale_code,
        TO_CHAR(s.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM') AS sale_month_year,
        TO_CHAR(s.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo', 'DD-MM-YYYY') AS data_solicitacao,
        c.name AS customer_name,
        s.is_event,
        s.turbo
    FROM sales s
    LEFT JOIN companies c ON s.company_id = c.id
    WHERE s.status IS DISTINCT FROM 'CANCELED'
),
fornecedor_info AS (
    SELECT 
        sipi.sale_item_id,
        ss.trading_name AS fornecedor_produto,
        sc.trading_name AS fornecedor_personalizacao
    FROM sale_item_production_infos sipi
    LEFT JOIN suppliers ss ON sipi.product_supplier_id = ss.id
    LEFT JOIN suppliers sc ON sipi.product_customizer_id = sc.id
),
tempo_producao_util AS (
    SELECT 
        e1.sale_item_id,
        e1.event_created_at_sao_paulo AS inicio,
        e2.event_created_at_sao_paulo AS fim,
        CASE
            WHEN e1.event_created_at_sao_paulo::time <= '18:00:00' AND DATE_TRUNC('day', e1.event_created_at_sao_paulo) = DATE_TRUNC('day', e2.event_created_at_sao_paulo) THEN
                EXTRACT(EPOCH FROM (e2.event_created_at_sao_paulo - e1.event_created_at_sao_paulo)) / 3600
            WHEN e1.event_created_at_sao_paulo::time > '18:00:00' AND DATE_TRUNC('day', e1.event_created_at_sao_paulo) = DATE_TRUNC('day', e2.event_created_at_sao_paulo) THEN
                EXTRACT(EPOCH FROM (e2.event_created_at_sao_paulo - e1.event_created_at_sao_paulo)) / 3600
            WHEN e1.event_created_at_sao_paulo::time > '18:00:00' AND DATE_TRUNC('day', e2.event_created_at_sao_paulo) = DATE_TRUNC('day', e1.event_created_at_sao_paulo + INTERVAL '1 day') THEN
                EXTRACT(EPOCH FROM (e2.event_created_at_sao_paulo - (DATE_TRUNC('day', e1.event_created_at_sao_paulo) + INTERVAL '1 day' + INTERVAL '9 hours'))) / 3600
            WHEN e1.event_created_at_sao_paulo::time <= '18:00:00' AND DATE_TRUNC('day', e2.event_created_at_sao_paulo) = DATE_TRUNC('day', e1.event_created_at_sao_paulo + INTERVAL '1 day') THEN
                (EXTRACT(EPOCH FROM (e2.event_created_at_sao_paulo - (DATE_TRUNC('day', e1.event_created_at_sao_paulo) + INTERVAL '1 day' + INTERVAL '9 hours'))) / 3600) +
                (EXTRACT(EPOCH FROM ((DATE_TRUNC('day', e1.event_created_at_sao_paulo) + INTERVAL '18 hours') - e1.event_created_at_sao_paulo)) / 3600)
            WHEN e1.event_created_at_sao_paulo::time > '18:00:00' AND DATE_TRUNC('day', e2.event_created_at_sao_paulo) > DATE_TRUNC('day', e1.event_created_at_sao_paulo + INTERVAL '1 day') THEN
                (EXTRACT(EPOCH FROM (e2.event_created_at_sao_paulo - (DATE_TRUNC('day', e2.event_created_at_sao_paulo) + INTERVAL '9 hours'))) / 3600) +
                (SELECT COUNT(*) 
                 FROM generate_series(DATE_TRUNC('day', e1.event_created_at_sao_paulo) + INTERVAL '1 day', 
                                      DATE_TRUNC('day', e2.event_created_at_sao_paulo) - INTERVAL '1 day', 
                                      INTERVAL '1 day') AS gs(day)
                 WHERE EXTRACT(DOW FROM gs.day) NOT IN (0, 6)
                   AND gs.day NOT IN (SELECT holiday_date FROM feriados)) * 9
            WHEN e1.event_created_at_sao_paulo::time <= '18:00:00' AND DATE_TRUNC('day', e2.event_created_at_sao_paulo) > DATE_TRUNC('day', e1.event_created_at_sao_paulo + INTERVAL '1 day') THEN
                (EXTRACT(EPOCH FROM (e2.event_created_at_sao_paulo - (DATE_TRUNC('day', e2.event_created_at_sao_paulo) + INTERVAL '9 hours'))) / 3600) +
                (EXTRACT(EPOCH FROM ((DATE_TRUNC('day', e1.event_created_at_sao_paulo) + INTERVAL '18 hours') - e1.event_created_at_sao_paulo)) / 3600) +
                (SELECT COUNT(*) 
                 FROM generate_series(DATE_TRUNC('day', e1.event_created_at_sao_paulo) + INTERVAL '1 day', 
                                      DATE_TRUNC('day', e2.event_created_at_sao_paulo) - INTERVAL '1 day', 
                                      INTERVAL '1 day') AS gs(day)
                 WHERE EXTRACT(DOW FROM gs.day) NOT IN (0, 6)
                   AND gs.day NOT IN (SELECT holiday_date FROM feriados)) * 9
        END AS tempo_uteis_producao
    FROM ordered_events e1
    JOIN ordered_events e2 ON e1.sale_item_id = e2.sale_item_id
    WHERE e1.status IN ('TO_PRODUCE', 'IN_PRODUCTION', 'IN_QUOTATION')
      AND e2.status IN ('QUALITY', 'DELIVERED')
      AND e2.event_created_at_sao_paulo > e1.event_created_at_sao_paulo
),
final AS (
    SELECT 
        f.sale_id,
        f.sale_item_id,
        f.event_month_year,
        sf.sale_month_year AS month_year,
        sf.data_solicitacao,
        f.previous_created_at,
        f.current_created_at,
        f.hours_between_status_teste,
        f.avg_hours_between_status,
        sf.sale_code AS code,
        sf.customer_name AS customer,
        pi.sku,
        pi.categoria_produto,
        pi.product_name AS nameproduct,
        CAST(pi.product_id AS TEXT) || CAST(sf.sale_code AS TEXT) AS item_pedido,
        fi.fornecedor_produto,
        CASE f.previous_status
            WHEN 'WAITING' THEN 'Aguardando'
            WHEN 'OUT_OF_STOCK' THEN 'Sem estoque'
            WHEN 'WITHOUT_ART' THEN 'Sem arte'
            WHEN 'ART_REQUIRED' THEN 'Sem arte'
            WHEN 'WITHOUT_GRADE' THEN 'Sem grade'
            WHEN 'GRID_REQUIRED' THEN 'Sem grade'
            WHEN 'IN_ADJUSTMENT' THEN 'Em ajuste'
            WHEN 'TO_PRODUCE' THEN 'Para produção'
            WHEN 'IN_QUOTATION' THEN 'Cotação'
            WHEN 'IN_PRODUCTION' THEN 'Em produção'
            WHEN 'IN_CUSTOMIZATION' THEN 'Personalização'
            WHEN 'WITHDRAWAL' THEN 'Retirada'
            WHEN 'LOGISTICS' THEN 'Logística'
            WHEN 'QUALITY' THEN 'Qualidade'
            WHEN 'DIVERGENT' THEN 'Divergente'
            WHEN 'DELIVERED' THEN 'Concluído'
            WHEN 'CANCELED' THEN 'Cancelado'
            ELSE f.previous_status
        END AS previous_status,
        CASE f.current_status
            WHEN 'WAITING' THEN 'Aguardando'
            WHEN 'OUT_OF_STOCK' THEN 'Sem estoque'
            WHEN 'WITHOUT_ART' THEN 'Sem arte'
            WHEN 'ART_REQUIRED' THEN 'Sem arte'
            WHEN 'WITHOUT_GRADE' THEN 'Sem grade'
            WHEN 'GRID_REQUIRED' THEN 'Sem grade'
            WHEN 'IN_ADJUSTMENT' THEN 'Em ajuste'
            WHEN 'TO_PRODUCE' THEN 'Para produção'
            WHEN 'IN_QUOTATION' THEN 'Cotação'
            WHEN 'IN_PRODUCTION' THEN 'Em produção'
            WHEN 'IN_CUSTOMIZATION' THEN 'Personalização'
            WHEN 'WITHDRAWAL' THEN 'Retirada'
            WHEN 'LOGISTICS' THEN 'Logística'
            WHEN 'QUALITY' THEN 'Qualidade'
            WHEN 'DIVERGENT' THEN 'Divergente'
            WHEN 'DELIVERED' THEN 'Concluído'
            WHEN 'CANCELED' THEN 'Cancelado'
            ELSE f.current_status
        END AS current_status,
        tpu.tempo_uteis_producao
    FROM (
        SELECT 
            event_month_year,
            previous_status,
            current_status,
            MIN(sale_id::text)::uuid AS sale_id,
            MIN(sale_item_id) AS sale_item_id,
            previous_created_at,
            current_created_at,
            hours_between_status_teste,
            AVG(hours_between_status) AS avg_hours_between_status
        FROM filtered_status_pairs
        WHERE (previous_status <> last_previous_status OR current_status <> last_current_status)
          AND previous_status IS NOT NULL AND current_status IS NOT NULL
          AND previous_status <> current_status
        GROUP BY event_month_year, previous_status, current_status, hours_between_status_teste, previous_created_at, current_created_at
    ) f
    LEFT JOIN sale_info sf ON f.sale_id = sf.sale_id
    LEFT JOIN product_info pi ON f.sale_item_id = pi.sale_item_id
    LEFT JOIN fornecedor_info fi ON f.sale_item_id = fi.sale_item_id
    LEFT JOIN tempo_producao_util tpu ON f.sale_item_id = tpu.sale_item_id
),
final_com_rank AS (
    SELECT 
        fnl.sale_id,
        fnl.sale_item_id,
        fnl.event_month_year,
        fnl.month_year,
        fnl.previous_created_at,
        fnl.current_created_at,
        fnl.hours_between_status_teste,
        fnl.avg_hours_between_status,
        fnl.code,
        fnl.customer,
        fnl.sku,
        fnl.categoria_produto,
        fnl.nameproduct,
        fnl.item_pedido,
        fnl.fornecedor_produto,
        fnl.previous_status,
        fnl.current_status,
        fnl.tempo_uteis_producao,
        fnl.data_solicitacao,  -- << certinho
        si.amount AS unidades_compradas,
        CASE
            WHEN si.amount <= 50 THEN '1 - Até 50'
            WHEN si.amount <= 100 THEN '2 - 51-100'
            WHEN si.amount <= 250 THEN '3 - 101-250'
            WHEN si.amount <= 500 THEN '4 - 251-500'
            WHEN si.amount <= 1000 THEN '5 - 501-1000'
            ELSE '6 - Acima de 1000'
        END AS faixa_unidades,
        CASE 
            WHEN sf.is_event = TRUE AND sf.turbo = TRUE THEN 'TURBO+EVENTO'
            WHEN sf.is_event = TRUE THEN 'EVENTO'
            WHEN sf.turbo = TRUE THEN 'TURBO'
            ELSE 'PADRAO'
        END AS "TURBOouEVENTO",
        ROW_NUMBER() OVER (PARTITION BY fnl.item_pedido ORDER BY fnl.previous_created_at DESC) AS row_num,
        COUNT(*) OVER (PARTITION BY fnl.item_pedido) AS total_rows
    FROM final fnl
    LEFT JOIN sale_items si ON fnl.sale_item_id = si.id
    LEFT JOIN sale_info sf ON fnl.sale_id = sf.sale_id  
)

SELECT
    event_month_year,
    month_year,
    code AS codigo_venda,
    customer AS nome_cliente,
    sku,
    nameproduct AS nome_sku,
    item_pedido,
    fornecedor_produto,
    previous_status,
    current_status,
    concat(previous_status, '->', current_status) AS variacao_status,
    previous_created_at,
    current_created_at,
    --fnl.data_solicitacao AS "Data da Solicitação",  -- <<< aqui corrigido
    hours_between_status_teste,
    avg_hours_between_status,
    faixa_unidades,
    categoria_produto,
    "TURBOouEVENTO" AS "Turbo ou Evento",
    CASE 
        WHEN row_num = 1 THEN tempo_uteis_producao
        ELSE NULL
    END AS tempo_uteis_producao
FROM final_com_rank fnl
WHERE month_year >= '2025-01'
ORDER BY codigo_venda, previous_created_at;

