CREATE TABLE Classe (
	IDClasse INT IDENTITY(1,1) NOT NULL,
	Descricao VARCHAR(255),
	CodClasse VARCHAR(50), -- e.g., 'MBA', 'ATE'
	PRIMARY KEY (IDClasse)
);

CREATE TABLE Asteroide (
	IDAsteroide INT IDENTITY(1,1) NOT NULL,
	number INT,                 -- Asteroid Number (e.g., 1, 99942)
	spkid VARCHAR(20),          -- Nullable because MPCORB doesn't have it
	pdes VARCHAR(20),           -- Provisional Designation
	name VARCHAR(100),
	prefix VARCHAR(10),
	neo BIT,                    -- Near Earth Object flag
	pha BIT,                    -- Potentially Hazardous Asteroid flag
	H DECIMAL(10, 5),           -- Absolute Magnitude
	G DECIMAL(10, 5),           -- Slope Parameter
	diameter DECIMAL(10, 5),
	diameter_sigma DECIMAL(10, 5),
	albedo DECIMAL(10, 5),
	PRIMARY KEY (IDAsteroide)
);

-- Indexes for Asteroide
CREATE UNIQUE NONCLUSTERED INDEX IX_Asteroide_spkid ON Asteroide(spkid) WHERE spkid IS NOT NULL;
CREATE INDEX IX_Asteroide_pdes ON Asteroide(pdes);
CREATE INDEX IX_Asteroide_number ON Asteroide(number) WHERE number IS NOT NULL;
CREATE INDEX IX_Asteroide_Flags ON Asteroide(pha, neo) INCLUDE (diameter, H);

CREATE TABLE Orbita (
	IDOrbita INT IDENTITY(1,1) NOT NULL,
	IDAsteroide INT NOT NULL,
	epoch DATE,                 -- Epoch of orbital elements
	e DECIMAL(20, 10),          -- Eccentricity
	sigma_e DECIMAL(30, 10),
	a DECIMAL(30, 10),          -- Semi-major axis
	sigma_a DECIMAL(30, 10),
	q DECIMAL(30, 10),          -- Perihelion distance
	sigma_q DECIMAL(30, 10),
	i DECIMAL(20, 10),          -- Inclination
	sigma_i DECIMAL(30, 10),
	om DECIMAL(20, 10),         -- Longitude of ascending node
	sigma_om DECIMAL(30, 10),
	w DECIMAL(20, 10),          -- Argument of perihelion
	sigma_w DECIMAL(30, 10),
	ma DECIMAL(20, 10),         -- Mean anomaly
	sigma_ma DECIMAL(30, 10),
	ad DECIMAL(30, 10),         -- Aphelion distance
	sigma_ad DECIMAL(30, 10),
	n DECIMAL(20, 10),          -- Mean motion
	sigma_n DECIMAL(30, 10),
	tp DATETIME2(7),            -- Time of perihelion passage
	sigma_tp DECIMAL(30, 10),
	per DECIMAL(30, 10),        -- Period
	sigma_per DECIMAL(30, 10),
	moid DECIMAL(20, 10),       -- Minimum Orbit Intersection Distance (AU)
	moid_ld DECIMAL(20, 10),    -- MOID in Lunar Distances
	rms DECIMAL(10, 5),
	uncertainty VARCHAR(10),    -- U parameter
	Reference VARCHAR(50),
	Num_Obs INT,                -- Number of Observations
	Num_Opp INT,                -- Number of Oppositions
	Arc VARCHAR(20),            -- Observation arc
	Coarse_Perts VARCHAR(20),   -- Coarse Perturbers
	Precise_Perts VARCHAR(20),  -- Precise Perturbers
	Hex_Flags VARCHAR(10),      -- MPCORB Hex Flags
	Is1kmNEO BIT,
	IsCriticalList BIT,
	IsOneOppositionEarlier BIT,
	IDClasse INT,
	PRIMARY KEY (IDOrbita),
	FOREIGN KEY (IDAsteroide) REFERENCES Asteroide(IDAsteroide),
	FOREIGN KEY (IDClasse) REFERENCES Classe(IDClasse)
);

-- Indexes for Orbita
CREATE UNIQUE INDEX UQ_Orbita_Asteroide_Epoch ON Orbita(IDAsteroide, epoch) WITH (IGNORE_DUP_KEY = ON);
CREATE INDEX IX_Orbita_Alerts ON Orbita(moid, rms, epoch) INCLUDE (e, i, a);
GO

CREATE TABLE Centro_de_observacao (
	IDCentro INT IDENTITY(1,1) NOT NULL,
	Nome VARCHAR(100),
	Localizacao VARCHAR(100),
	PRIMARY KEY (IDCentro)
);

CREATE TABLE Astronomo (
	IDAstronomo INT IDENTITY(1,1) NOT NULL,
	Nome VARCHAR(100),
	IDCentro INT NOT NULL,
	PRIMARY KEY (IDAstronomo),
	FOREIGN KEY (IDCentro) REFERENCES Centro_de_observacao(IDCentro)
);

CREATE TABLE Equipamento (
	IDEquipamento INT IDENTITY(1,1) NOT NULL,
	Nome VARCHAR(100),
	Tipo VARCHAR(50),
	IDCentro INT NOT NULL,
	PRIMARY KEY (IDEquipamento),
	FOREIGN KEY (IDCentro) REFERENCES Centro_de_observacao(IDCentro)
);

CREATE TABLE Software (
	IDSoftware INT IDENTITY(1,1) NOT NULL,
	Nome VARCHAR(100),
	Versao VARCHAR(20),
	PRIMARY KEY (IDSoftware)
);

CREATE TABLE Observacao (
	IDObservacao INT IDENTITY(1,1) NOT NULL,
	IDAsteroide INT NOT NULL,
	IDAstronomo INT,
	IDEquipamento INT,
	IDSoftware INT,
	Data_atualizacao DATE,
	Hora TIME,
	Duracao DECIMAL(10, 2),
	Modo VARCHAR(50),
	PRIMARY KEY (IDObservacao),
	FOREIGN KEY (IDAsteroide) REFERENCES Asteroide(IDAsteroide),
	FOREIGN KEY (IDAstronomo) REFERENCES Astronomo(IDAstronomo),
	FOREIGN KEY (IDEquipamento) REFERENCES Equipamento(IDEquipamento),
	FOREIGN KEY (IDSoftware) REFERENCES Software(IDSoftware)
);

CREATE TABLE Imagem (
	ID_Imagem INT IDENTITY(1,1) NOT NULL,
	IDAsteroide INT NOT NULL,
	URL VARCHAR(255),
	Data_imagem DATE,
	Descricao TEXT,
	PRIMARY KEY (ID_Imagem),
	FOREIGN KEY (IDAsteroide) REFERENCES Asteroide(IDAsteroide)
);

CREATE TABLE Alerta (
	ID_Alerta INT IDENTITY(1,1) NOT NULL,
	IDAsteroide INT NOT NULL,
	Data_Alerta DATE,
	Prioridade VARCHAR(20),
	Nivel INT,
	Descricao TEXT,
	Estado VARCHAR(20),
	PRIMARY KEY (ID_Alerta),
	FOREIGN KEY (IDAsteroide) REFERENCES Asteroide(IDAsteroide)
);