-- 1. ¿Cuántas filas totales hay ahora?
SELECT count(*) FROM gastos_fact_silver;

-- 2. ¿Cuántas métricas únicas se crearon?
-- Si aquí salen cosas que no son montos (ej. nombres de columnas de texto), el script se equivocó
SELECT DISTINCT tipo_metrica FROM gastos_fact_silver;

-- 3. ¿El monto total cuadra con el original?
-- Suma el PIA de un año y compáralo con tu Excel/CSV original
SELECT SUM(monto) FROM gastos_fact_silver WHERE tipo_metrica = 'pia' AND anio = 2024;