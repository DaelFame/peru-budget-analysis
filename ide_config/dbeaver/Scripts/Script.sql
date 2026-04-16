SELECT COUNT(*) 
FROM gastos_raw_bronze b
LEFT JOIN dim_institucion i ON b.sec_ejec = i.sec_ejec 
WHERE i.id_institucion IS NULL;