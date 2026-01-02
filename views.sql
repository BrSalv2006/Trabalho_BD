CREATE OR ALTER VIEW vw_Last5Detected AS
SELECT TOP 5
	a.IDAsteroide,
	a.spkid,
	a.pdes,
	a.name,
	a.diameter,
	a.pha,
	a.neo,
	o.epoch AS data_referencia,
	o.moid_ld
FROM Asteroide a
JOIN Orbita o ON a.IDAsteroide = o.IDAsteroide
ORDER BY a.IDAsteroide DESC;
GO

CREATE OR ALTER VIEW vw_PHA_NEO AS
SELECT
	a.IDAsteroide,
	a.spkid,
	a.pdes,
	a.name,
	a.diameter,
	a.H AS magnitude_absoluta,
	o.moid_ld,
	o.rms
FROM Asteroide a
JOIN Orbita o ON a.IDAsteroide = o.IDAsteroide
WHERE a.neo = 1 AND a.pha = 1;
GO

CREATE OR ALTER VIEW vw_TopCenters AS
SELECT TOP 10
	c.Nome AS Centro,
	c.Localizacao,
	COUNT(o.IDObservacao) AS total_observacoes
	FROM Centro_de_observacao c
	JOIN Equipamento e ON c.IDCentro = e.IDCentro
	JOIN Observacao o ON e.IDEquipamento = o.IDEquipamento
	GROUP BY c.Nome, c.Localizacao
	ORDER BY total_observacoes DESC;
	GO

CREATE OR ALTER VIEW vw_LargestPHAs AS
SELECT TOP 20
	a.IDAsteroide,
	a.spkid,
	a.pdes,
	a.name,
	a.diameter,
	a.albedo,
	o.moid_ld,
	o.tp AS data_perielio
FROM Asteroide a
JOIN Orbita o ON a.IDAsteroide = o.IDAsteroide
WHERE a.pha = 1
ORDER BY a.diameter DESC;
GO

CREATE OR ALTER VIEW vw_EstatisticasAlerta AS
SELECT
	Nivel,
	CASE Nivel
		WHEN 4 THEN 'Vermelho (Crítico)'
		WHEN 3 THEN 'Laranja (Perigo)'
		WHEN 2 THEN 'Amarelo (Atenção)'
		WHEN 1 THEN 'Verde (Normal)'
		ELSE 'Sem classificação'
	END AS Legenda,
COUNT(*) AS Quantidade
FROM Alerta
WHERE Estado = 'Ativo'
GROUP BY Nivel;
GO

CREATE OR ALTER VIEW vw_ProximosEventosCriticos AS
SELECT TOP 10
	a.IDAsteroide,
	a.spkid,
	a.pdes,
	a.name,
	o.tp AS data_aproximacao,
	o.moid_ld AS distancia_lunar,
	a.diameter
FROM Orbita o
JOIN Asteroide a ON o.IDAsteroide = a.IDAsteroide
WHERE o.moid_ld < 5
	AND CAST(o.tp AS DATE) >= CAST(GETDATE() AS DATE)
ORDER BY o.tp ASC;
GO

CREATE OR ALTER VIEW vw_EstatisticasDescoberta AS
SELECT
	COUNT(a.IDAsteroide) AS Novos_neos_ultimo_mes
FROM Asteroide a
JOIN Orbita o ON a.IDAsteroide = o.IDAsteroide
WHERE a.neo = 1
	AND CAST(o.epoch as DATE) BETWEEN DATEADD(MONTH, -1, GETDATE()) AND GETDATE();
GO

CREATE OR ALTER VIEW vw_EvolucaoPrecisao AS
SELECT
	YEAR(epoch) AS Ano,
	AVG(rms) AS rms_medio
FROM Orbita
WHERE epoch IS NOT NULL
GROUP BY YEAR(epoch);
GO