select
status ,
c."name" ,
   s.code as  codigo_venda,
           additional_value as receita_adicional,
           storage_price as receita_armazenamento,
           shipment_price as receita_envio,
    additional_value_description as desc_receita_adicional
 from sales s  
 left join   companies c ON c.id = s.company_id
 where s.status  not IN ('CANCELED','TO-BILL')
--and s.created_at >= '2024-01-01'
-- s.code = '1170'
 and (additional_value>0 or storage_price>0 or shipment_price>0)
order by code desc
