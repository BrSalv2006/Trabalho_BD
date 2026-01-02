CREATE OR ALTER FUNCTION dbo.fn_Torino_Simplificada (
	@diameter DECIMAL(10,5),
	@moid_ld DECIMAL(20,10),
	@rms DECIMAL(10,5),
	@tp DATETIME2(7),
	@pha BIT
)
RETURNS INT
AS
BEGIN
	DECLARE @hoje DATE = CAST(GETDATE() AS DATE);
	DECLARE @data_evento DATE = CAST(@tp AS DATE);

	IF @diameter > 0.03
	   AND @moid_ld < 1
	   AND @data_evento BETWEEN @hoje AND DATEADD(DAY,30,@hoje)
		RETURN 4;

	IF @diameter > 0.05
	   AND @moid_ld < 5
	   AND @rms < 0.3
		RETURN 3;

	IF @diameter > 0.1
	   AND @moid_ld BETWEEN 5 AND 20
	   AND @data_evento BETWEEN @hoje and DATEADD(DAY, 180, @hoje)
		RETURN 2;

	IF @pha = 1
	   AND @diameter BETWEEN 0.05 AND 0.5
	   AND @moid_ld BETWEEN 20 AND 100
		RETURN 1;

	RETURN 0;
END;
GO

CREATE TRIGGER trg_alerta_alta_prioridade
ON Orbita
AFTER INSERT, UPDATE
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @hoje DATE = CAST(GETDATE() AS DATE);

	INSERT INTO Alerta (IDAsteroide, Data_Alerta, Prioridade, Nivel, Descricao, Estado)
	SELECT
		i.IDAsteroide,
		@hoje,
		'Alta',
		dbo.fn_Torino_Simplificada(a.diameter, i.moid_ld, i.rms, i.tp, a.pha),
		'Aproximação iminente',
		'Ativo'
	FROM inserted i
	JOIN Asteroide a ON a.IDAsteroide = i.IDAsteroide
	WHERE i.moid_ld < 1
	  AND a.diameter > 0.01
	  AND CAST(i.tp AS DATE) BETWEEN @hoje AND DATEADD(DAY, 7, @hoje)
	  AND NOT EXISTS (SELECT 1 FROM Alerta al WHERE al.IDAsteroide = i.IDAsteroide AND al.Descricao = 'Aproximação iminente' AND al.Estado = 'Ativo');

	INSERT INTO Alerta (IDAsteroide, Data_Alerta, Prioridade, Nivel, Descricao, Estado)
	SELECT
		i.IDAsteroide, @hoje,
		'Alta',
		dbo.fn_Torino_Simplificada(a.diameter, i.moid_ld, i.rms, i.tp, a.pha),
		'PHA com trajetória incerta',
		'Ativo'
	FROM inserted i
	JOIN Asteroide a ON a.IDAsteroide = i.IDAsteroide
	WHERE a.pha = 1
	  AND a.diameter > 0.1
	  AND i.rms > 0.8
	  AND i.moid_ld < 20
	  AND NOT EXISTS (SELECT 1 FROM Alerta al WHERE al.IDAsteroide = i.IDAsteroide AND al.Descricao = 'PHA com trajetória incerta' AND al.Estado = 'Ativo');
END;
GO

CREATE TRIGGER trg_alerta_media_prioridade
ON Orbita
AFTER INSERT, UPDATE
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @hoje DATE = CAST(GETDATE() AS DATE);

	INSERT INTO Alerta (IDAsteroide, Data_Alerta, Prioridade, Nivel, Descricao, Estado)
	SELECT
		i.IDAsteroide, @hoje,
		'Média',
		dbo.fn_Torino_Simplificada(a.diameter, i.moid_ld, i.rms, i.tp, a.pha),
		'Novo asteróide de grande porte',
		'Ativo'
	FROM inserted i
	JOIN Asteroide a ON a.IDAsteroide = i.IDAsteroide
	WHERE a.diameter > 0.5
	  AND i.moid_ld < 50
	  AND NOT EXISTS (SELECT 1 FROM Alerta al WHERE al.IDAsteroide = i.IDAsteroide AND al.Descricao = 'Novo asteróide de grande porte' AND al.Estado = 'Ativo');

	INSERT INTO Alerta (IDAsteroide, Data_Alerta, Prioridade, Nivel, Descricao, Estado)
	SELECT
		i.IDAsteroide, @hoje,
		'Média',
		dbo.fn_Torino_Simplificada(a.diameter, i.moid_ld, i.rms, i.tp, a.pha),
		'Mudança orbital significativa',
		'Ativo'
	FROM inserted i
	JOIN deleted d ON i.IDOrbita = d.IDOrbita
	JOIN Asteroide a ON a.IDAsteroide = i.IDAsteroide
	WHERE (ABS(i.e - d.e) > 0.05 OR ABS(i.i - d.i) > 2)
	  AND NOT EXISTS (SELECT 1 FROM Alerta al WHERE al.IDAsteroide = i.IDAsteroide AND al.Descricao = 'Mudança orbital significativa' AND al.Estado = 'Ativo');
END;
GO

CREATE TRIGGER trg_alerta_baixa_prioridade
ON Orbita
AFTER INSERT, UPDATE
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @hoje DATE = CAST(GETDATE() AS DATE);

	INSERT INTO Alerta (IDAsteroide, Data_Alerta, Prioridade, Nivel, Descricao, Estado)
	SELECT
		i.IDAsteroide, @hoje,
		'Baixa',
		dbo.fn_Torino_Simplificada(a.diameter, i.moid_ld, i.rms, i.tp, a.pha),
		'Características anómalas',
		'Ativo'
	FROM inserted i
	JOIN Asteroide a ON a.IDAsteroide = i.IDAsteroide
	WHERE a.albedo > 0.3
	  AND i.e > 0.8
	  AND i.i > 70
	  AND a.diameter > 0.2
	  AND NOT EXISTS (SELECT 1 FROM Alerta al WHERE al.IDAsteroide = i.IDAsteroide AND al.Descricao = 'Características anómalas' AND al.Estado = 'Ativo');

	INSERT INTO Alerta (IDAsteroide, Data_Alerta, Prioridade, Nivel, Descricao, Estado)
	SELECT
		i.IDAsteroide, @hoje,
		'Baixa',
		dbo.fn_Torino_Simplificada(a.diameter, i.moid_ld, i.rms, i.tp, a.pha),
		'Agrupamento temporal de aproximações',
		'Ativo'
	FROM inserted i
	JOIN Asteroide a ON a.IDAsteroide = i.IDAsteroide
	WHERE i.moid_ld < 10
	  AND (
		  SELECT COUNT(*)
		  FROM Orbita o
		  WHERE o.moid_ld < 10
			AND MONTH(o.tp) = MONTH(i.tp)
			AND YEAR(o.tp) = YEAR(i.tp)
	  ) > 5
	  AND NOT EXISTS (SELECT 1 FROM Alerta al WHERE al.IDAsteroide = i.IDAsteroide AND al.Descricao = 'Agrupamento temporal de aproximações' AND al.Estado = 'Ativo');
END;
GO