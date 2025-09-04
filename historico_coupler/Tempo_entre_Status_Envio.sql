WITH unpacked_events AS (
    SELECT
        so.id AS shipping_order_id,
        so.code AS code,
        so.created_at AS order_created_at,
        v.created_at AS event_created_at,
        COALESCE(
            v.object_changes->'status'->>1, 
            v.object->>'status' 
        ) AS event_type
    FROM shipping_orders so
    INNER JOIN versions v ON 
        v.item_id = so.id::varchar
    WHERE so.created_at::date > '2024-01-01'
        --AND so.code = '15232'
),
ordered_events AS (
    SELECT 
        shipping_order_id,
        code,
        order_created_at,
        TO_CHAR(event_created_at, 'YYYY-MM') AS event_month_year,
        event_type AS status,
        event_created_at,
        ROW_NUMBER() OVER (PARTITION BY shipping_order_id ORDER BY event_created_at, event_type) AS rn
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
        e1.shipping_order_id,
        e1.code,
        e1.event_month_year,
        e1.status AS previous_status,
        e2.status AS current_status,
        e1.event_created_at AS previous_created_at,
        e2.event_created_at AS current_created_at,
        case  
	    WHEN -- quando o evento começa e termina no mesmo dia antes das 18h
	    EXTRACT(HOUR FROM e1.event_created_at) <= 18 and EXTRACT(DAY FROM (DATE_TRUNC('day', e2.event_created_at) - DATE_TRUNC('day', e1.event_created_at))) =0
	    then 1
	    WHEN -- quando o evento começa e termina no mesmo dia após as 18h
	    EXTRACT(HOUR FROM e1.event_created_at) >= 18 and EXTRACT(DAY FROM (DATE_TRUNC('day', e2.event_created_at) - DATE_TRUNC('day', e1.event_created_at))) =0
	    then
	     2
        WHEN -- quando o evento começa após as 18h e termina no dia seguinte
        EXTRACT(HOUR FROM e1.event_created_at) >= 18 and EXTRACT(DAY FROM (DATE_TRUNC('day', e2.event_created_at) - DATE_TRUNC('day', e1.event_created_at))) = 1
        THEN 
            3
        when -- quando o evento começa antes as 18h e termina no dia seguinte
        EXTRACT(HOUR FROM e1.event_created_at) <= 18 and EXTRACT(DAY FROM (DATE_TRUNC('day', e2.event_created_at) - DATE_TRUNC('day', e1.event_created_at))) = 1
        then 
        4
        WHEN -- quando o evento começa após as 18h e é concluido varios dias depois
        EXTRACT(HOUR FROM e1.event_created_at) >= 18 and EXTRACT(DAY FROM (DATE_TRUNC('day', e2.event_created_at) - DATE_TRUNC('day', e1.event_created_at))) > 1
        THEN 
            5
                     WHEN -- quando o evento começa antes as 18h e é concluido varios dias depois
        EXTRACT(HOUR FROM e1.event_created_at) < 18 and EXTRACT(DAY FROM (DATE_TRUNC('day', e2.event_created_at) - DATE_TRUNC('day', e1.event_created_at))) > 1
        THEN 
            6
        END AS hours_between_status_teste,
     case
	    WHEN -- quando o evento começa e termina no mesmo dia antes das 18h
	    EXTRACT(HOUR FROM e1.event_created_at) <= 18 and EXTRACT(DAY FROM (DATE_TRUNC('day', e2.event_created_at) - DATE_TRUNC('day', e1.event_created_at))) =0
	    then EXTRACT(EPOCH FROM NULLIF(e2.event_created_at - e1.event_created_at, INTERVAL '0 second')) / 3600
	    WHEN -- quando o evento começa e termina no mesmo dia após as 18h
	    EXTRACT(HOUR FROM e1.event_created_at) >= 18 and EXTRACT(DAY FROM (DATE_TRUNC('day', e2.event_created_at) - DATE_TRUNC('day', e1.event_created_at))) =0
	    then
	     EXTRACT(EPOCH FROM NULLIF(e2.event_created_at - e1.event_created_at, INTERVAL '0 second')) / 3600
        WHEN -- quando o evento começa após as 18h e termina no dia seguinte
        EXTRACT(HOUR FROM e1.event_created_at) >= 18 and EXTRACT(DAY FROM (DATE_TRUNC('day', e2.event_created_at) - DATE_TRUNC('day', e1.event_created_at))) = 1
        THEN 
            EXTRACT(EPOCH FROM (e2.event_created_at - (DATE_TRUNC('day', e1.event_created_at) + INTERVAL '1 day' + INTERVAL '9 hours'))) / 3600
        when -- quando o evento começa antes as 18h e termina no dia seguinte
        EXTRACT(HOUR FROM e1.event_created_at) <= 18 and EXTRACT(DAY FROM (DATE_TRUNC('day', e2.event_created_at) - DATE_TRUNC('day', e1.event_created_at))) = 1
        then 
        (EXTRACT(EPOCH FROM (e2.event_created_at - (DATE_TRUNC('day', e1.event_created_at) + INTERVAL '1 day' + INTERVAL '9 hours'))) / 3600) + 
        (EXTRACT(EPOCH FROM ((DATE_TRUNC('day', e1.event_created_at) + INTERVAL '1 day' + INTERVAL '9 hours')) - ((DATE_TRUNC('day', e1.event_created_at) + INTERVAL '1 day' + INTERVAL '9 hours'))) / 3600)
        WHEN -- quando o evento começa após as 18h e é concluido varios dias depois
        EXTRACT(HOUR FROM e1.event_created_at) >= 18 and EXTRACT(DAY FROM (DATE_TRUNC('day', e2.event_created_at) - DATE_TRUNC('day', e1.event_created_at))) > 1
        THEN 
            (EXTRACT(EPOCH FROM (e2.event_created_at - (DATE_TRUNC('day', e2.event_created_at) + INTERVAL '9 hours'))) / 3600) +
            (SELECT COUNT(*) 
             FROM generate_series(DATE_TRUNC('day', e1.event_created_at) + INTERVAL '1 day', 
                                  DATE_TRUNC('day', e2.event_created_at) - INTERVAL '1 day', 
                                  INTERVAL '1 day') AS gs(day) 
             WHERE EXTRACT(DOW FROM gs.day) NOT IN (0, 6) 
               AND gs.day NOT IN (SELECT holiday_date FROM feriados)) * 14
        WHEN  -- quando o evento começa antes das 18h e é concluído vários dias depois
        EXTRACT(HOUR FROM e1.event_created_at) <= 18 
        AND EXTRACT(DAY FROM (DATE_TRUNC('day', e2.event_created_at) - DATE_TRUNC('day', e1.event_created_at))) > 1
        THEN 
            (EXTRACT(EPOCH FROM (e2.event_created_at - (DATE_TRUNC('day', e2.event_created_at) + INTERVAL '9 hours'))) / 3600) +
            (EXTRACT(EPOCH FROM ((DATE_TRUNC('day', e1.event_created_at) + INTERVAL '18 hours') - e1.event_created_at)) / 3600) +
            (SELECT COUNT(*) 
             FROM generate_series(DATE_TRUNC('day', e1.event_created_at) + INTERVAL '1 day', 
                                  DATE_TRUNC('day', e2.event_created_at) - INTERVAL '1 day', 
                                  INTERVAL '1 day') AS gs(day) 
             WHERE EXTRACT(DOW FROM gs.day) NOT IN (0, 6) 
               AND gs.day NOT IN (SELECT holiday_date FROM feriados)) * 14
    END AS hours_between_status
    FROM ordered_events e1
    LEFT JOIN ordered_events e2 
        ON e1.shipping_order_id = e2.shipping_order_id
        AND e1.rn = e2.rn - 1
    WHERE e1.status IS NOT NULL AND e2.status IS NOT NULL
        AND e2.event_created_at >= e1.event_created_at
),
filtered_status_pairs AS (
    SELECT * ,
        LAG(previous_status) OVER (PARTITION BY shipping_order_id ORDER BY previous_created_at) AS last_previous_status,
        LAG(current_status) OVER (PARTITION BY shipping_order_id ORDER BY current_created_at) AS last_current_status
    FROM status_pairs
),
final as
(SELECT 
    event_month_year,
    CASE previous_status
	    WHEN 'draft' THEN 'Rascunho'
	    WHEN 'waiting_logistics' THEN 'Aguardando logística'
	    WHEN 'waiting_picking' THEN 'Aguardando picking'
	    WHEN 'waiting_mounting' THEN 'Aguardando montagem'
	    WHEN 'waiting_labels' THEN 'Aguardando etiquetas'
	    WHEN 'waiting_for_printing' THEN 'Aguardando impressão'
	    WHEN 'ready_to_ship' THEN 'Pronto para envio'
	    WHEN 'shipped' THEN 'Enviado'
	    WHEN 'finished' THEN 'Concluído'
	    WHEN 'waiting_addresses' THEN 'Aguardando endereços'
	    WHEN 'waiting_items_arrival' THEN 'Aguardando chegada de itens'
	    WHEN 'canceled' THEN 'Cancelado'
	    WHEN 'mounting_with_divergence' THEN 'Montagem com Divergência'
	    WHEN 'error' THEN 'Erro'
	    WHEN 'withdrawn' THEN 'Retirado pelo cliente'
	    WHEN 'ready_for_pickup' THEN 'Pronto para retirada pelo cliente'
	    WHEN 'mounting' THEN 'Em montagem'
	    ELSE previous_status
	END AS previous_status,
	CASE current_status
	    WHEN 'draft' THEN 'Rascunho'
	    WHEN 'waiting_logistics' THEN 'Aguardando logística'
	    WHEN 'waiting_picking' THEN 'Aguardando picking'
	    WHEN 'waiting_mounting' THEN 'Aguardando montagem'
	    WHEN 'waiting_labels' THEN 'Aguardando etiquetas'
	    WHEN 'waiting_for_printing' THEN 'Aguardando impressão'
	    WHEN 'ready_to_ship' THEN 'Pronto para envio'
	    WHEN 'shipped' THEN 'Enviado'
	    WHEN 'finished' THEN 'Concluído'
	    WHEN 'waiting_addresses' THEN 'Aguardando endereços'
	    WHEN 'waiting_items_arrival' THEN 'Aguardando chegada de itens'
	    WHEN 'canceled' THEN 'Cancelado'
	    WHEN 'mounting_with_divergence' THEN 'Montagem com Divergência'
	    WHEN 'error' THEN 'Erro'
	    WHEN 'withdrawn' THEN 'Retirado pelo cliente'
	    WHEN 'ready_for_pickup' THEN 'Pronto para retirada pelo cliente'
	    WHEN 'mounting' THEN 'Em montagem'
	    ELSE current_status
	END AS current_status,
    STRING_AGG(DISTINCT code::TEXT, ', ') AS codes,  -- Conversão explícita de code para text
        previous_created_at,
    current_created_at,
    hours_between_status_teste as hours_between_status_case,
    AVG(hours_between_status) AS avg_hours_between_status
FROM filtered_status_pairs
WHERE (previous_status <> last_previous_status OR current_status <> last_current_status)
  AND previous_status IS NOT NULL AND current_status IS NOT NULL
  AND previous_status <> current_status
GROUP BY event_month_year, previous_status, current_status,hours_between_status_teste,previous_created_at,current_created_at
ORDER BY STRING_AGG(DISTINCT code::TEXT, ', '), previous_created_at)
select
event_month_year,
codes,
previous_status,
current_status,
concat(previous_status,'->',current_status) as "variacao_status",
previous_created_at,
current_created_at,
hours_between_status_case,
avg_hours_between_status
from final
