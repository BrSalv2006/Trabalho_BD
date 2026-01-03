PRINT 'A inserir TODOS os dados fictícios (Auxiliares + Asteroides de Teste)...';

-- =================================================================================
-- PARTE 1: DADOS AUXILIARES (Centros, Equipamentos, Software, Astronomos)
-- =================================================================================

-- 1. CENTRO DE OBSERVAÇÃO
INSERT INTO Centro_de_observacao (Nome, Localizacao) VALUES
('Mauna Kea Observatory', 'Hawaii, USA'),
('Palomar Observatory', 'California, USA'),
('Roque de los Muchachos Observatory', 'La Palma, Spain'),
('Paranal Observatory', 'Atacama, Chile'),
('Kitt Peak National Observatory', 'Arizona, USA'),
('Siding Spring Observatory', 'NSW, Australia'),
('Catalina Sky Survey', 'Arizona, USA'),
('Pan-STARRS', 'Hawaii, USA'),
('La Silla Observatory', 'Coquimbo, Chile'),
('Lowell Observatory', 'Arizona, USA');

-- 2. EQUIPAMENTO
INSERT INTO Equipamento (Nome, Tipo, IDCentro) VALUES
('Keck I Telescope', 'Optical', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Mauna Kea Observatory')),
('Hale Telescope', 'Optical', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Palomar Observatory')),
('Gran Telescopio Canarias', 'Optical', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Roque de los Muchachos Observatory')),
('VLT Unit Telescope 1', 'Optical/Infrared', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Paranal Observatory')),
('Mayall Telescope', 'Optical', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Kitt Peak National Observatory')),
('Anglo-Australian Telescope', 'Optical', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Siding Spring Observatory')),
('Schmidt Telescope', 'Wide-field', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Catalina Sky Survey')),
('PS1 Telescope', 'Wide-field', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Pan-STARRS')),
('3.6m Telescope', 'Optical', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'La Silla Observatory')),
('Discovery Channel Telescope', 'Optical', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Lowell Observatory'));

-- 3. SOFTWARE
INSERT INTO Software (Nome) VALUES
('Astrometrica'),
('Find_Orb'),
('Celestia'),
('Stellarium'),
('SAOImage DS9');

-- 4. ASTRONOMO
INSERT INTO Astronomo (Nome, IDCentro) VALUES
('Dr. Sarah Connor', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Mauna Kea Observatory')),
('Dr. Alan Grant', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Palomar Observatory')),
('Dr. Ellie Sattler', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Roque de los Muchachos Observatory')),
('Dr. Ian Malcolm', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Paranal Observatory')),
('Dr. Emmett Brown', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Kitt Peak National Observatory')),
('Dr. Eleanor Arroway', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Siding Spring Observatory')),
('Dr. Dave Bowman', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Catalina Sky Survey')),
('Dr. Ryan Stone', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Pan-STARRS')),
('Dr. Mark Watney', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'La Silla Observatory')),
('Dr. Cooper', (SELECT IDCentro FROM Centro_de_observacao WHERE Nome = 'Lowell Observatory'));

PRINT 'Dados auxiliares base inseridos.';

-- =================================================================================
-- PARTE 2: ASTEROIDES DE TESTE E ORBITAS (Para disparar alertas)
-- =================================================================================

-- CASO 1: ALTA PRIORIDADE - "Aproximação Iminente"
INSERT INTO Asteroide (spkid, pdes, name, neo, pha, diameter, albedo)
VALUES ('9900001', '2026 DOOM', 'Teste Iminente', 1, 1, 0.5, 0.1);

DECLARE @ID1 INT = SCOPE_IDENTITY();

INSERT INTO Orbita (IDAsteroide, epoch, tp, moid_ld, rms, e, a, q, i, om, w, ma, n)
VALUES (
	@ID1,
	'2026-01-01',
	'2026-01-08',
	0.5,
	0.1, 0.2, 1.5, 1.2, 5.0, 10, 20, 30, 0.2
);

-- CASO 2: ALTA PRIORIDADE - "PHA com Trajetória Incerta"
INSERT INTO Asteroide (spkid, pdes, name, neo, pha, diameter, albedo)
VALUES ('9900002', '2026 LOST', 'Teste Incerto', 1, 1, 0.15, 0.1);

DECLARE @ID2 INT = SCOPE_IDENTITY();

INSERT INTO Orbita (IDAsteroide, epoch, tp, moid_ld, rms, e, a, q, i, om, w, ma, n)
VALUES (
	@ID2,
	'2026-01-01',
	'2026-02-15',
	15.0,
	0.95,
	0.2, 1.5, 1.2, 5.0, 10, 20, 30, 0.2
);

-- CASO 3: MÉDIA PRIORIDADE - "Novo Asteroide de Grande Porte"
INSERT INTO Asteroide (spkid, pdes, name, neo, pha, diameter, albedo)
VALUES ('9900003', '2026 BIG', 'Teste Gigante', 1, 0, 0.8, 0.1);

DECLARE @ID3 INT = SCOPE_IDENTITY();

INSERT INTO Orbita (IDAsteroide, epoch, tp, moid_ld, rms, e, a, q, i, om, w, ma, n)
VALUES (
	@ID3,
	'2026-01-01',
	'2026-01-20',
	40.0,
	0.1, 0.2, 1.5, 1.2, 5.0, 10, 20, 30, 0.2
);

-- CASO 4: MÉDIA PRIORIDADE - "Mudança Orbital Significativa"
INSERT INTO Asteroide (spkid, pdes, name, neo, pha, diameter, albedo)
VALUES ('9900004', '2026 SHIFT', 'Teste Mudanca', 1, 0, 0.2, 0.1);

DECLARE @ID4 INT = SCOPE_IDENTITY();

INSERT INTO Orbita (IDAsteroide, epoch, tp, moid_ld, rms, e, a, q, i, om, w, ma, n)
VALUES (
	@ID4,
	'2026-01-01',
	'2026-03-01',
	60.0,
	0.1,
	0.2,
	1.5, 1.2, 5.0, 10, 20, 30, 0.2
);

UPDATE Orbita
SET e = 0.3
WHERE IDAsteroide = @ID4;

-- CASO 5: BAIXA PRIORIDADE - "Características Anómalas"
INSERT INTO Asteroide (spkid, pdes, name, neo, pha, diameter, albedo)
VALUES ('9900005', '2026 WEIRD', 'Teste Anomalo', 1, 0, 0.4, 0.5); -- Albedo > 0.3

DECLARE @ID5 INT = SCOPE_IDENTITY();

INSERT INTO Orbita (IDAsteroide, epoch, tp, moid_ld, rms, e, a, q, i, om, w, ma, n)
VALUES (
	@ID5,
	'2026-01-01',
	'2026-04-01',
	60.0,
	0.1,
	0.85,
	2.5, 0.5,
	75.0,
	10, 20, 30, 0.2
);

-- CASO 6: BAIXA PRIORIDADE - "Agrupamento Temporal de Aproximações"
DECLARE @i INT = 1;
WHILE @i <= 6
BEGIN
	INSERT INTO Asteroide (spkid, pdes, name, neo, pha, diameter, albedo)
	VALUES (
		CONCAT('990001', @i + 10), -- +10 para evitar conflito com IDs anteriores
		CONCAT('2026 GRP', @i),
		CONCAT('Teste Grupo ', @i),
		1, 0, 0.1, 0.1
	);

	DECLARE @ID_Grupo INT = SCOPE_IDENTITY();

	INSERT INTO Orbita (IDAsteroide, epoch, tp, moid_ld, rms, e, a, q, i, om, w, ma, n)
	VALUES (
		@ID_Grupo,
		'2026-01-01',
		'2026-01-15',
		5.0,
		0.1, 0.2, 1.5, 1.2, 5.0, 10, 20, 30, 0.2
	);

	SET @i = @i + 1;
END;

-- CASO 7: SEM ALERTA - "Seguro"
INSERT INTO Asteroide (spkid, pdes, name, neo, pha, diameter, albedo)
VALUES ('9900099', '2026 SAFE', 'Teste Seguro', 0, 0, 0.1, 0.1);

DECLARE @ID_Safe INT = SCOPE_IDENTITY();

INSERT INTO Orbita (IDAsteroide, epoch, tp, moid_ld, rms, e, a, q, i, om, w, ma, n)
VALUES (
    @ID_Safe,
    '2026-01-01',
    '2026-06-01',
    100.0,
    0.1, 0.2, 1.5, 1.2, 5.0, 10, 20, 30, 0.2
);

PRINT 'Asteroides de teste inseridos.';

-- =================================================================================
-- PARTE 3: DADOS DEPENDENTES (Imagens e Observações para os asteroides de teste)
-- =================================================================================

-- 5. IMAGEM
INSERT INTO Imagem (IDAsteroide, URL, Data_imagem, Descricao)
SELECT TOP 10
    IDAsteroide,
    CONCAT('/images/ast_', spkid, '_', ABS(CHECKSUM(NEWID())) % 1000, '.jpg'),
    DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 30, GETDATE()),
    'Imagem de descoberta/confirmação'
FROM Asteroide
WHERE spkid LIKE '99000%'
ORDER BY NEWID();

-- 6. OBSERVAÇÃO
INSERT INTO Observacao (IDAsteroide, IDAstronomo, IDSoftware, IDEquipamento, Data_atualizacao, Hora, Duracao, Modo)
SELECT TOP 10
    a.IDAsteroide,
    (SELECT TOP 1 IDAstronomo FROM Astronomo ORDER BY NEWID()),
    (SELECT TOP 1 IDSoftware FROM Software ORDER BY NEWID()),
    (SELECT TOP 1 IDEquipamento FROM Equipamento ORDER BY NEWID()),
    DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 10, GETDATE()),
    '22:00:00',
    2.5,
    'Automático'
FROM Asteroide a
WHERE a.spkid LIKE '99000%'
ORDER BY NEWID();

PRINT 'Todos os dados de teste foram inseridos com sucesso!';
GO
