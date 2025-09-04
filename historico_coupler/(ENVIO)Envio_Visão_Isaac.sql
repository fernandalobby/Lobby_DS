WITH seguidores AS (
    SELECT
        sof.shipping_order_id,
        STRING_AGG(DISTINCT CONCAT(u.first_name, ' ', u.last_name), ', ') AS solicitantes
    FROM shipping_orders_followers sof
    LEFT JOIN users u ON sof.follower_id = u.id
    GROUP BY sof.shipping_order_id
),
itens_envio AS (
    SELECT 
    	si.shipment_id,
        cp.sku,
        STRING_AGG(DISTINCT cp.name, ', ') AS observacao,
        STRING_AGG(DISTINCT p.name, ', ') AS produto_catalogo,
        STRING_AGG(DISTINCT COALESCE(p.name, cp.name), ', ') AS produto_nome,
        SUM(COALESCE(cps.inventory_amount, cp.unique_inventory_amount)) AS estoque,
        STRING_AGG(DISTINCT si."size", ', ' ORDER BY si."size") AS "size",
        SUM(si.quantity) AS produto_quantidade
    FROM shipment_items si
    LEFT JOIN customer_products cp ON si.customer_product_id = cp.id
    LEFT JOIN products p ON cp.product_id = p.id
    LEFT JOIN customer_product_sizes cps 
        ON cps.customer_product_id = cp.id AND cps.id::varchar = si."size"
    GROUP BY si.shipment_id, cp.sku
),
entrega_cliente AS (
    SELECT 
        item_id::uuid AS shipment_id,
        MIN(created_at) AS data_entrega_cliente
    FROM versions
    WHERE event = 'update'
      AND item_type = 'Shipment'
      AND object_changes::jsonb -> 'status' ->> 1 = 'DELIVERED'
    GROUP BY item_id
),

frete_por_shipment AS (
    SELECT
        so.code AS "Pedido de Envio",
        sh.recipient_state AS "Estado",
        sh.recipient_name AS "Destinatário",
        SUM(sh.shipment_price) AS frete_total
    FROM shipments sh
    LEFT JOIN shipping_orders so ON sh.shipping_order_id = so.id
    WHERE 
        sh.deleted_at IS NULL
        AND so.deleted_at IS NULL
    GROUP BY so.code, sh.recipient_state, sh.recipient_name
),
envios AS (
    SELECT 
        seg.solicitantes AS "Nome Completo",
        so.id AS shipping_order_id,
        so.code AS "Pedido de Envio",
        c.name AS "Cliente",
        seg.solicitantes AS "Solicitante",
        t.name AS "Time",
        so.title AS pedido_envio,
        so.total_kits_quantity AS quantidade_kits,
        EXTRACT(MONTH FROM so.created_at AT TIME ZONE 'America/Sao_Paulo') AS mes,
        EXTRACT(YEAR FROM so.created_at AT TIME ZONE 'America/Sao_Paulo') AS ano,
        sh.recipient_name AS "Destinatário", 
        sh.tracking_code AS "Código de Rastreio", 
        STRING_AGG(DISTINCT ck.name, ', ') AS "Kit do Cliente",
        so.shipping_purpose AS "Objetivo do Envio",
        COALESCE(fps.frete_total, 0) AS "Frete",
        sh.recipient_city AS "Cidade", 
        sh.recipient_state AS "Estado",
        TO_CHAR(so.created_at AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM-DD') AS "Data da Solicitação",
        TO_CHAR(so.shipment_expected_to AT TIME ZONE 'America/Sao_Paulo', 'DD-MM-YYYY') AS "Data do Envio",
        sh.status AS "Status", 
        so.created_through AS "Meio de Solicitação",
        ie.produto_quantidade,
        ie.sku,
        ie.produto_nome,
        ie.observacao,
        ie.produto_catalogo,
        ie.estoque,
        sh.recipient_zipcode AS "CEP", 
        sh.recipient_street AS "Logradouro",
        sh.recipient_number AS "Número",
        sh.recipient_complement AS "Complemento",
        sh.recipient_neighborhood AS "Bairro",
        sh.shipment_category AS "Categoria de Envio", 
        ss.company_name AS "Transportadora", 
        ss.name AS "Método de Envio", 
        MAX(sh.days_to_deliver) AS "Dias para Entrega",
        sh.service_bill_id AS "service_bill_id",
        motivo_devolucao.motivo_devolucao,
        CASE  
            WHEN sh.shipped_at IS NOT NULL AND ec.data_entrega_cliente IS NOT NULL
            THEN ROUND(EXTRACT(EPOCH FROM (ec.data_entrega_cliente - sh.shipped_at)) / 86400)::int
            ELSE NULL
        END AS "Tempo de Entrega em Dias"
    FROM shipments sh
    LEFT JOIN shipping_services ss ON sh.shipping_service_id = ss.id 
    LEFT JOIN shipping_orders so ON sh.shipping_order_id = so.id
    LEFT JOIN companies c ON so.company_id = c.id
    LEFT JOIN shipping_orders_customer_kits sock ON so.id = sock.shipping_order_id
    LEFT JOIN customer_kits ck ON ck.id = sock.customer_kit_id
    LEFT JOIN seguidores seg ON so.id = seg.shipping_order_id
    LEFT JOIN teams t ON t.id = so.team_id
    LEFT JOIN itens_envio ie ON sh.id = ie.shipment_id
    --LEFT JOIN itens_envio ie ON ie.sku = cp.sku
    --LEFT JOIN customer_products cp ON cp.sku = ie.sku
    LEFT JOIN entrega_cliente ec ON ec.shipment_id = sh.id
    LEFT JOIN frete_por_shipment fps 
	  ON fps."Pedido de Envio" = so.code 
	  AND fps."Estado" = sh.recipient_state 
	  AND fps."Destinatário" = sh.recipient_name
    LEFT JOIN LATERAL (
        SELECT string_agg(DISTINCT evento->>'description', ', ') AS motivo_devolucao
        FROM jsonb_array_elements(sh.tracking_events::jsonb) AS evento
        WHERE evento->>'description' ILIKE '%Objeto não entregue%'
    ) motivo_devolucao ON TRUE
    WHERE 
        so.status NOT IN ('CANCELED', 'canceled')
        AND sh.deleted_at IS NULL
        AND so.deleted_at IS NULL
        AND sh.shipment_category IN ('SHIPPING', 'DEVOLUTION_RESEND')
    GROUP BY 
        so.id, so.code, so.shipment_expected_to, sh.status, c.name, so.created_at, so.title, 
        sh.recipient_name, sh.recipient_zipcode, sh.recipient_state,so.total_kits_quantity,
        sh.shipment_category, ss.company_name, ss.name, sh.shipment_price, sh.recipient_city, 
        so.shipping_purpose, so.created_through, t.name, sh.tracking_code, seg.solicitantes,
        sh.declared_value, sh.recipient_street, sh.recipient_number, sh.recipient_complement,
        sh.recipient_neighborhood, sh.service_bill_id, 
        ie.sku, ie.produto_quantidade, ie.produto_nome, ie.observacao, ie.produto_catalogo, ie.estoque,
        motivo_devolucao.motivo_devolucao , sh.shipped_at, ec.data_entrega_cliente,fps.frete_total
),

classificacao_final AS (
    SELECT
        e."Solicitante",
        e."Nome Completo",
        e."Pedido de Envio",
        e."Destinatário",
        e."Kit do Cliente",
        e."Frete",
        e.mes,
        e.ano,
        e."Cliente",
        e."Time",
        e."Tempo de Entrega em Dias",
        e.sku,
        e.produto_nome,
        e.produto_catalogo,
        e.observacao,
        e.estoque,
        e.produto_quantidade,
        e.quantidade_kits, 
        e.pedido_envio AS kit,
        CASE
            WHEN e."Status" = 'PACKAGE_RETURNED' THEN 'DEVOLVIDO'
            WHEN e."Status" = 'DELIVERED' THEN 'ENTREGUE'
            WHEN e."Status" = 'HELD_FOR_PICKUP' THEN 'AGUARDANDO RETIRADA'
            WHEN e."Status" = 'TRANSIT' THEN 'EM TRÂNSITO'
            WHEN e."Status" = 'SOLVED' THEN 'RESOLVIDO'
            WHEN e."Status" = 'DEVIATION' THEN 'DESVIO'
            WHEN e."Status" = 'DELAYED' THEN 'EM ATRASO'
            WHEN e."Status" = 'DELIVERY_ROUTE' THEN 'EM TRÂNSITO'
            ELSE e."Status"
        END AS status_traduzido,
        e."Estado",
        COALESCE(e.motivo_devolucao, '') AS motivo_devolucao
    FROM envios e
),
produto_agrupado AS (
    SELECT
        "Pedido de Envio",
        "Estado",
        "Destinatário",
        sku,
        produto_nome,
        produto_catalogo,
        observacao,
        estoque,
        SUM(produto_quantidade) AS produto_quantidade,
        quantidade_kits,
        --tipo_kit,
        kit,
        status_traduzido,
        "Solicitante",
        "Nome Completo",
        "Kit do Cliente",
        "Frete",
        "Tempo de Entrega em Dias",
        mes,
        ano,
        "Cliente",
        "Time",
        motivo_devolucao
    FROM classificacao_final
    GROUP BY
        "Pedido de Envio", "Estado", "Destinatário", sku,
        produto_nome, produto_catalogo, observacao, estoque,
        quantidade_kits, kit, status_traduzido,
        "Solicitante", "Nome Completo", "Kit do Cliente",
        "Frete", "Tempo de Entrega em Dias",
        mes, ano, "Cliente", "Time",motivo_devolucao
),
soma_total_por_pedido AS (
    SELECT "Pedido de Envio", SUM(produto_quantidade) AS total_prod
    FROM produto_agrupado
    GROUP BY "Pedido de Envio"
),
kits_por_estado AS (
    SELECT
        cf."Pedido de Envio",
        cf."Estado",
        cf."Destinatário",
        ROUND(
            SUM(cf.produto_quantidade) * MAX(cf.quantidade_kits)::numeric /
            NULLIF(st.total_prod, 0)
        ) AS qtd_kits_estado
    FROM produto_agrupado cf
    JOIN soma_total_por_pedido st ON st."Pedido de Envio" = cf."Pedido de Envio"
    GROUP BY cf."Pedido de Envio", cf."Estado", cf."Destinatário", st.total_prod
)
SELECT 
    cf.mes AS "Mês",
    cf.ano AS "Ano",
    cf."Cliente",
    cf."Time",
    cf."Kit do Cliente" AS "Kit",
    cf.produto_nome AS "Produto",
    cf.produto_catalogo AS "Produto - Catálogo Lobby",
    cf.observacao AS "Observações",
    cf.sku AS "SKU",
    cf.produto_quantidade AS "Quantidade",
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY cf."Pedido de Envio", cf."Estado" ORDER BY cf.produto_nome) = 1 THEN
            kpe.qtd_kits_estado
        ELSE NULL
    END AS "Qtd de Kits",
    cf.status_traduzido AS "Status",
    cf."Estado",
    cf.kit AS "Nome do Kit",
    cf.motivo_devolucao AS "Motivo",
    cf."Solicitante",
    cf."Destinatário",
    cf."Pedido de Envio" AS "Código do Pedido de Envio",
    CASE 
        WHEN ROW_NUMBER() OVER (
            PARTITION BY cf."Pedido de Envio", cf."Estado", cf."Destinatário"
            ORDER BY cf.produto_nome
        ) = 1 THEN cf."Frete"
        ELSE NULL
    END AS "Valor do Frete",
    CASE 
        WHEN ROW_NUMBER() OVER (
            PARTITION BY cf."Pedido de Envio", cf."Estado", cf."Destinatário"
            ORDER BY cf.produto_nome
        ) = 1 THEN cf."Tempo de Entrega em Dias"
        ELSE NULL
    END AS "Tempo de Entrega em Dias"
FROM produto_agrupado cf
LEFT JOIN kits_por_estado kpe
    ON cf."Pedido de Envio" = kpe."Pedido de Envio"
    AND cf."Estado" = kpe."Estado"
    AND cf."Destinatário" = kpe."Destinatário"
WHERE cf."Cliente" = 'Isaac'
--and cf."Pedido de Envio" = '18441'
ORDER BY 
    cf.ano, cf.mes, cf."Cliente", cf."Time", cf."Kit do Cliente", cf.produto_nome, cf.kit, cf."Pedido de Envio"; 
