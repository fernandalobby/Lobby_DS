-- CTE 1: Dias úteis com feriados
WITH dias_uteis AS (
  SELECT day::DATE AS dia_util
  FROM generate_series('2023-01-01'::date, '2025-12-31'::date, '1 day') day
  WHERE EXTRACT(DOW FROM day) NOT IN (0,6)
    AND day NOT IN (
      '2024-01-01', '2024-01-02','2024-01-02', '2024-02-12','2024-02-13','2024-03-29','2024-04-21','2024-05-01','2024-05-30',
      '2024-09-07', '2024-10-12','2024-11-02','2024-11-15','2024-12-25','2024-12-24', '2024-12-30','2024-12-31',
      '2025-01-03', '2025-01-02', '2025-01-01','2025-03-03','2025-03-04','2025-04-18','2025-04-21','2025-05-01','2025-06-19',
      '2025-09-07','2025-10-12','2025-11-02','2025-11-15','2025-12-25','2025-12-24', '2025-12-30','2025-12-31'
    )
),

-- CTE 2: Soma dos pedidos
pedido_soma AS (
  SELECT
    sh.id AS shipment_id,
    sh.shipping_order_id,
    sh.shipped_at::date AS data_envio,
    SUM(si.quantity) AS soma_pecas,
    COUNT(DISTINCT si.customer_product_id) AS num_skus
  FROM shipments sh
  LEFT JOIN shipment_items si ON sh.id = si.shipment_id
  WHERE sh.deleted_at IS NULL
  GROUP BY sh.id, sh.shipping_order_id, sh.shipped_at::date
),
-- CTE 3: Base com joins e agregações
base AS (
  SELECT
    so.code,
so.id AS shipping_order_id,
    c.name AS cliente,
    COALESCE(p.name, 'Sem Plano') AS plano,
    CASE
      WHEN COUNT(DISTINCT psoma.num_skus) = 1 AND MAX(psoma.num_skus) = 0 THEN 'Admin - Envio Sem SKU'
      WHEN so.created_through = 'customer_app' THEN 'App'
      WHEN so.created_through = 'redeem' THEN 'Resgate'
      WHEN so.created_through = 'admin_app' THEN 'Admin - Casos com Erro Inclusos'
      ELSE 'Indefinido'
    END AS tipo_envio,
    so.created_at AT TIME ZONE 'America/Sao_Paulo' AS created_at,
    so.shipment_expected_to AT TIME ZONE 'America/Sao_Paulo' AS shipment_expected_to,
    shipped_at AT TIME ZONE 'America/Sao_Paulo' AS shipped_at,
    so.total_kits_quantity,
    sh.id AS shipment_id,
    EXTRACT(MONTH FROM so.created_at AT TIME ZONE 'America/Sao_Paulo') AS mês_created_at,
    EXTRACT(YEAR FROM so.created_at AT TIME ZONE 'America/Sao_Paulo') AS ano_created_at,
    CASE
      WHEN shipped_at IS NOT NULL THEN EXTRACT(MONTH FROM shipped_at AT TIME ZONE 'America/Sao_Paulo')
      ELSE NULL
    END AS mês_shipped_at,
    CASE
      WHEN shipped_at IS NOT NULL THEN EXTRACT(YEAR FROM shipped_at AT TIME ZONE 'America/Sao_Paulo')
      ELSE NULL
    END AS ano_shipped_at,
    EXTRACT(WEEK FROM so.created_at AT TIME ZONE 'America/Sao_Paulo') AS num_semana_created_at,
    psoma.soma_pecas AS soma_quantidade_pecas,
    psoma.num_skus AS num_skus
  FROM shipping_orders so
  LEFT JOIN shipments sh ON sh.shipping_order_id = so.id
  LEFT JOIN pedido_soma psoma ON psoma.shipment_id = sh.id
  LEFT JOIN shipping_services ss ON sh.shipping_service_id = ss.id
  LEFT JOIN companies c ON so.company_id = c.id
  LEFT JOIN subscriptions sub ON c.id = sub.company_id
  LEFT JOIN plans p ON sub.plan_id = p.id
  WHERE so.status NOT IN ('canceled', 'draft')
    AND so.created_at > '2023-01-01'
    AND sh.deleted_at IS NULL
  GROUP BY
    so.code, c.name, p.name, so.created_through,so.id,
    so.created_at, so.shipment_expected_to, shipped_at,so.id,
    so.total_kits_quantity, sh.id, psoma.soma_pecas, psoma.num_skus
),

-- CTE 4: Dias úteis entre criação e envio
uteis_diff AS (
  SELECT
    b.code, b.cliente, b.plano, b.tipo_envio,b.shipping_order_id,
    b.created_at, b.shipped_at, b.total_kits_quantity,
    b.soma_quantidade_pecas, b.num_skus, b.shipment_id,
    b.mês_created_at, b.ano_created_at,
    b.mês_shipped_at, b.ano_shipped_at,
    b.num_semana_created_at,
    COUNT(d.dia_util) AS dias_uteis_entre_criacao_e_envio
  FROM base b
  LEFT JOIN dias_uteis d ON d.dia_util BETWEEN b.created_at::date AND b.shipped_at::date
  GROUP BY
    b.code, b.cliente, b.plano, b.tipo_envio,
    b.created_at, b.shipped_at, b.total_kits_quantity,
    b.soma_quantidade_pecas, b.num_skus, b.shipment_id,
    b.mês_created_at, b.ano_created_at,
    b.mês_shipped_at, b.ano_shipped_at,
    b.num_semana_created_at,b.shipping_order_id
),

-- CTE 5: Versions
primeira_previsao_cliente AS (
  SELECT
    item_id::uuid AS shipment_id,
    MIN(created_at) AS data_alteracao,
    MIN((object_changes::jsonb -> 'shipment_expected_to') ->> 1) AS primeira_previsao
  FROM versions
  WHERE event = 'update'
    AND object_changes::jsonb ? 'shipment_expected_to'
  GROUP BY item_id
),

-- CTE 6: Prazo esperado (D+N úteis)
prazo_esperado AS (
  SELECT
    u.*,
    COALESCE(
      ppc.primeira_previsao::date,
      (
        SELECT dia_util
        FROM dias_uteis d
        WHERE d.dia_util >= u.created_at::date
        ORDER BY dia_util
        LIMIT 1
      )
    ) AS saida_esperada_cliente,

    (
      SELECT dia_util
      FROM (
        SELECT dia_util,
               ROW_NUMBER() OVER (ORDER BY dia_util) AS rn
        FROM dias_uteis
        WHERE dia_util > (
          SELECT MAX(dia_util)
          FROM dias_uteis
          WHERE dia_util <=
            CASE
              WHEN u.created_at::time >= '18:00:00'
                THEN (u.created_at + INTERVAL '1 day')::date
              ELSE u.created_at::date
            END
        )
      ) ranked
      WHERE rn = CASE
        WHEN u.tipo_envio = 'App' THEN
          CASE
            WHEN LOWER(u.plano) LIKE '%enterprise%' THEN 1
            WHEN LOWER(u.plano) LIKE '%pró%' THEN 2
            WHEN LOWER(u.plano) LIKE '%essential%' THEN 3
            WHEN LOWER(u.plano) LIKE '%free%' THEN 3
            ELSE 3
          END
        WHEN u.tipo_envio IN ('Resgate', 'Admin - Casos com Erro Inclusos') THEN 3
        ELSE 3
      END
    ) AS saida_esperada_operacao

  FROM uteis_diff u
  LEFT JOIN base b ON u.code = b.code AND u.shipment_id = b.shipment_id
  LEFT JOIN primeira_previsao_cliente ppc ON u.shipment_id = ppc.shipment_id
),

-- CTE 7: Resultado final com atraso
resultado_final AS (
  SELECT *,
    CASE
      WHEN shipped_at::date > saida_esperada_cliente THEN 'sim'
      ELSE 'não'
    END AS atraso_visao_cliente,
    CASE
      WHEN shipped_at::date > saida_esperada_operacao AND shipped_at::date > saida_esperada_cliente THEN 'sim'
      ELSE 'não'
    END AS atraso_visao_operacao
  FROM prazo_esperado
),
-- CTE 8: Erros ShippingOrder com duração
erro AS (
  WITH entradas AS (
    SELECT
      v.item_id,
      v.created_at AS entrada_at
    FROM versions v
    WHERE v.item_type = 'ShippingOrder'
      AND v.object_changes->'status'->>-1 = 'error'
  ),
  saidas AS (
    SELECT
      v.item_id,
      v.created_at AS saida_at
    FROM versions v
    WHERE v.item_type = 'ShippingOrder'
      AND v.object_changes->'status'->>0 = 'error'
  ),
  duracoes AS (
    SELECT
      e.item_id,
      e.entrada_at,
      s.saida_at,
      ROUND(EXTRACT(EPOCH FROM (s.saida_at - e.entrada_at)) / 86400, 2) AS tempo_em_erro_dias,
      ROUND(EXTRACT(EPOCH FROM (s.saida_at - e.entrada_at)) / 3600, 2) AS tempo_em_erro_horas
    FROM entradas e
    JOIN saidas s
      ON e.item_id = s.item_id
     AND s.saida_at > e.entrada_at
  )
  SELECT DISTINCT ON (item_id, entrada_at)
    item_id::uuid AS shipping_order_id,
    entrada_at,
    saida_at,
    tempo_em_erro_dias,
    tempo_em_erro_horas,
    'sim' AS teve_erro
  FROM duracoes
)
SELECT
  rf.code AS "code",
  rf.shipping_order_id,
  rf.plano AS "Plano",
  rf.cliente AS "Cliente",
  rf.tipo_envio AS "Tipo Envio",
  TO_CHAR(MIN(rf.created_at), 'DD-MM-YYYY') AS "Criado em",
  TO_CHAR(MIN(rf.shipped_at)::date, 'DD-MM-YYYY') AS "Saida Efetiva (Data do Envio)",
  TO_CHAR(MIN(rf.saida_esperada_cliente), 'DD-MM-YYYY') AS "Saida esperada Cliente (data)",
  TO_CHAR(MIN(rf.saida_esperada_operacao), 'DD-MM-YYYY') AS "Saida Esperada Operação (data)",
  CASE WHEN BOOL_OR(rf.atraso_visao_cliente = 'sim') THEN 'sim' ELSE 'não' END AS "Atraso visão cliente (sim/não)",
  CASE WHEN BOOL_OR(rf.atraso_visao_operacao = 'sim') THEN 'sim' ELSE 'não' END AS "Atraso visão operação (sim/não)",
  MAX(rf.dias_uteis_entre_criacao_e_envio) AS "Dias (uteis) entre criação do pedido e envio",
  MIN(rf.mês_created_at) AS "Mês de Criação",
  MIN(rf.ano_created_at) AS "Ano de Criação",
  MIN(rf.mês_shipped_at) AS "Mês de Envio",
  MIN(rf.ano_shipped_at) AS "Ano de Envio",
  MIN(rf.num_semana_created_at) AS "num_semana_created_at",
  rf.num_skus AS "# SKUs",
  SUM(rf.soma_quantidade_pecas) AS soma_pecas,
  ROUND(
    MAX(rf.total_kits_quantity) * SUM(rf.soma_quantidade_pecas)
    / NULLIF(SUM(SUM(rf.soma_quantidade_pecas)) OVER (PARTITION BY rf.code), 0), 0
  ) AS "Kits enviados",
  CASE WHEN BOOL_OR(ecd.teve_erro = 'sim') THEN 'sim' ELSE 'não' END AS "Teve erro",
  ROUND(MAX(ecd.tempo_em_erro_dias), 2) AS "Tempo em erro (dias)",
  ROUND(MAX(ecd.tempo_em_erro_horas), 2) AS "Tempo em erro (horas)"

FROM resultado_final rf
LEFT JOIN erro ecd ON rf.shipping_order_id = ecd.shipping_order_id
WHERE rf.shipped_at IS NOT null
GROUP BY
  rf.code,
  rf.shipping_order_id,
  rf.plano,
  rf.cliente,
  rf.tipo_envio,
  rf.shipped_at::date,
  rf.num_skus
ORDER BY rf.code, rf.shipped_at::date;
