SELECT 
  id,
  trading_name,
  created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS created_at,
  updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS updated_at,
  storage_supplier,
  company_name,
  document_number,
  payment_period_in_days,

  -- Primeiro dia da semana (segunda-feira) com base em created_at
  (
    (created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') 
    - ((EXTRACT(DOW FROM (created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'))::int + 6) % 7) * INTERVAL '1 day'
  )::date AS semana_data,

  -- Semana formatada
  'Semana ' || TO_CHAR(
    (
      (created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') 
      - ((EXTRACT(DOW FROM (created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'))::int + 6) % 7) * INTERVAL '1 day'
    ), 
    'IW'
  ) || ' - ' || TO_CHAR(
    (
      (created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') 
      - ((EXTRACT(DOW FROM (created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'))::int + 6) % 7) * INTERVAL '1 day'
    ), 
    'DD/MM/YYYY'
  ) AS semana_formatada

FROM suppliers;
