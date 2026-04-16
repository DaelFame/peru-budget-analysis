SELECT 
    (SELECT COUNT(*) FROM dim_institucion) as total_entidades,
    (SELECT COUNT(*) FROM dim_geografia) as total_puntos_geograficos,
    (SELECT COUNT(*) FROM dim_clasificador) as total_tipos_gasto,
    (SELECT COUNT(*) FROM dim_programa) as total_programas_presupuestales;
