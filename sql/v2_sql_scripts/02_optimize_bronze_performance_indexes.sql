-- Creamos un índice sobre las llaves naturales que usamos en los JOINs
-- Esto acelerará drásticamente el proceso de Unpivot
CREATE INDEX idx_bronze_joins_lookup ON gastos_raw_bronze (
    sec_ejec, 
    ejecutora, 
    departamento_ejecutora, 
    provincia_ejecutora, 
    distrito_ejecutora, 
    generica, 
    especifica_det, 
    actividad_accion_obra
);