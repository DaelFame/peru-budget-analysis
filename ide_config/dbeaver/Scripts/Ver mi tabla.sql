SELECT tipo_monto, anio, COUNT(*), SUM(monto) 
FROM fct_ejecucion_gasto 
GROUP BY tipo_monto, anio 
ORDER BY anio, tipo_monto;