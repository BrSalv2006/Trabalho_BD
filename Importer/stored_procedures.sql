CREATE PROCEDURE dbo.sp_AlterarEstadoAlerta
    @ID_Alerta INT,
    @NovoEstado VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;
    IF @NovoEstado IN ('Ativo', 'Resolvido', 'Ignorado')
        UPDATE Alerta SET Estado = @NovoEstado WHERE ID_Alerta = @ID_Alerta;
    ELSE
        RAISERROR('Estado inválido!', 16, 1);
END;
GO