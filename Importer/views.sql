CREATE OR ALTER VIEW vw_EstatisticasAlerta AS
SELECT
	(SELECT COUNT(*) FROM Alerta WHERE Nivel = 4 AND Estado = 'Ativo') AS Alertas_Vermelhos,
	(SELECT COUNT(*) FROM Alerta WHERE Nivel = 3 AND Estado = 'Ativo') AS Alertas_Laranja,
	(SELECT COUNT(*) FROM Asteroide WHERE pha = 1 AND diameter > 0.1) AS Total_PHAs_Monitorizados_GT_100m;
GO

CREATE OR ALTER VIEW vw_ProximosEventosCriticos AS
SELECT TOP 5
	o.tp AS Data_Proxima_Aproximacao,
	a.IDAsteroide AS IDAsteroide,
	a.name AS Nome,
	a.pdes as Designacao_Provisoria,
	o.moid_ld AS Distancia_Lunar,
	a.diameter AS Diametro
FROM Orbita o
JOIN Asteroide a ON o.IDAsteroide = a.IDAsteroide
WHERE o.moid_ld IS NOT NULL
  AND o.moid_ld < 5
  AND o.tp IS NOT NULL
  AND CAST(o.tp AS DATE) >= CAST(GETDATE() AS DATE)
ORDER BY o.tp ASC;
GO

CREATE OR ALTER VIEW vw_EstatisticasDescoberta AS
SELECT
	COUNT(a.IDAsteroide) AS Novos_NEOs_ultimo_mes
FROM Asteroide a
JOIN Orbita o ON a.IDAsteroide = o.IDAsteroide
WHERE a.neo = 1
	AND CAST(o.epoch as DATE) BETWEEN DATEADD(MONTH, -1, GETDATE()) AND GETDATE();
GO

CREATE OR ALTER VIEW vw_EvolucaoPrecisao AS
SELECT TOP 1000
	YEAR(epoch) AS Ano,
	AVG(rms) AS rms_medio
FROM Orbita
WHERE epoch IS NOT NULL AND rms IS NOT NULL
GROUP BY YEAR(epoch)
ORDER BY Ano ASC;
GO

CREATE OR ALTER VIEW vw_Last5Detected AS
SELECT TOP 5
	a.IDAsteroide AS IDAsteroide,
	a.pdes AS Designacao_Provisoria,
	a.name AS Nome,
	o.epoch AS Data_Deteccao
FROM Asteroide a
JOIN Orbita o ON a.IDAsteroide = o.IDAsteroide
ORDER BY o.epoch DESC;
GO

CREATE OR ALTER VIEW vw_PHA_NEO AS
SELECT
	a.IDAsteroide AS IDAsteroide,
	a.pdes AS Designacao_Provisoria,
	a.name AS Nome,
	a.neo AS NEO,
	a.pha AS PHA
FROM Asteroide a
WHERE a.neo = 1 AND a.pha = 1;
GO

CREATE OR ALTER VIEW vw_TopCenters AS
SELECT TOP 10
	c.IDCentro AS IDCentro,
	c.Nome AS Nome,
	c.Localizacao AS Localizacao,
	COUNT(DISTINCT o.IDObservacao) AS total_observacoes
FROM Centro_de_observacao c
JOIN Equipamento e ON c.IDCentro = e.IDCentro
JOIN Observacao o ON o.IDEquipamento = e.IDEquipamento
GROUP BY c.IDCentro, c.Nome, c.Localizacao
ORDER BY total_observacoes DESC;
GO

CREATE OR ALTER VIEW vw_LargestPHAs AS
SELECT TOP 20
	a.IDAsteroide AS IDAsteroide,
	a.pdes AS Designacao_Provisoria,
	a.name AS Nome,
	a.diameter AS Diametro
FROM Asteroide a
WHERE a.pha = 1
ORDER BY a.diameter DESC;
GO