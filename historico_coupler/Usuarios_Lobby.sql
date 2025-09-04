SELECT
	CONCAT(u.first_name, ' ', u.last_name) AS Nome, c.name as Empresa, p.name as Plano, u.job_position as Posição, u.email as Email, u.phone as Telefone from users u
	LEFT JOIN
		companies c on u.company_id = c.id
    LEFT JOIN 
        subscriptions sub ON c.id = sub.company_id  
    LEFT JOIN 
        plans p ON sub.plan_id = p.id  
