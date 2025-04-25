CREATE DATABASE BioNet;
GO

USE BioNet;
GO

CREATE TABLE resultados_examenes (
    id INT IDENTITY PRIMARY KEY,
    laboratorio_id INT NOT NULL,
    paciente_id INT NOT NULL,
    tipo_examen VARCHAR(100) NOT NULL,
    resultado VARCHAR(100) NOT NULL,
    fecha_examen DATE NOT NULL,
    CONSTRAINT UQ_result UNIQUE(laboratorio_id, paciente_id, tipo_examen, fecha_examen)
);
GO

CREATE TABLE log_cambios_resultados (
    id INT IDENTITY PRIMARY KEY,
    operacion VARCHAR(10) NOT NULL, 
    paciente_id INT NOT NULL,
    tipo_examen VARCHAR(100) NOT NULL,
    fecha DATETIME NOT NULL DEFAULT GETDATE()
);
GO

CREATE TRIGGER trg_log_cambios
ON resultados_examenes
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Operacion VARCHAR(10);

    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
        SET @Operacion = 'INSERT';
    ELSE
        SET @Operacion = 'UPDATE';

    INSERT INTO log_cambios_resultados (operacion, paciente_id, tipo_examen, fecha)
    SELECT 
        @Operacion,
        i.paciente_id,
        i.tipo_examen,
        GETDATE()
    FROM inserted i;
END;
GO
