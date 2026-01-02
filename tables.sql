CREATE TABLE Classe (
	IDClasse INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	Descricao VARCHAR(255),
	CodClasse VARCHAR(50) UNIQUE
);
GO

CREATE TABLE Software (
	IDSoftware INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	Nome VARCHAR(100) UNIQUE
);
GO

CREATE TABLE Centro_de_observacao (
	IDCentro INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	Nome VARCHAR(100),
	Localizacao VARCHAR(100)
);
GO

CREATE TABLE Asteroide (
	IDAsteroide INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	number INT,
	spkid VARCHAR(20),
	pdes VARCHAR(20),
	name VARCHAR(100),
	prefix VARCHAR(10),
	neo BIT,
	pha BIT,
	H DECIMAL(10, 5),
	G DECIMAL(10, 5),
	diameter DECIMAL(10, 5),
	diameter_sigma DECIMAL(10, 5),
	albedo DECIMAL(10, 5)
);
GO

CREATE UNIQUE NONCLUSTERED INDEX IX_Asteroide_spkid ON Asteroide(spkid) WHERE spkid IS NOT NULL;
CREATE UNIQUE NONCLUSTERED INDEX IX_Asteroide_pdes ON Asteroide(pdes) WHERE pdes IS NOT NULL;
CREATE UNIQUE NONCLUSTERED INDEX IX_Asteroide_number ON Asteroide(number) WHERE number IS NOT NULL;
CREATE NONCLUSTERED INDEX IX_Asteroide_name ON Asteroide(name) WHERE name IS NOT NULL;
CREATE INDEX IX_Asteroide_Flags ON Asteroide(pha, neo) INCLUDE (diameter, H);
GO

CREATE TABLE Astronomo (
	IDAstronomo INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	Nome VARCHAR(100),
	IDCentro INT NOT NULL,
	FOREIGN KEY (IDCentro) REFERENCES Centro_de_observacao(IDCentro)
);
GO

CREATE TABLE Equipamento (
	IDEquipamento INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	Nome VARCHAR(100),
	Tipo VARCHAR(50),
	IDCentro INT NOT NULL,
	FOREIGN KEY (IDCentro) REFERENCES Centro_de_observacao(IDCentro)
);
GO

CREATE TABLE Orbita (
	IDOrbita INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	IDAsteroide INT NOT NULL,
	epoch DATE,
	e DECIMAL(20, 10), sigma_e DECIMAL(30, 10),
	a DECIMAL(30, 10), sigma_a DECIMAL(30, 10),
	q DECIMAL(30, 10), sigma_q DECIMAL(30, 10),
	i DECIMAL(20, 10), sigma_i DECIMAL(30, 10),
	om DECIMAL(20, 10), sigma_om DECIMAL(30, 10),
	w DECIMAL(20, 10), sigma_w DECIMAL(30, 10),
	ma DECIMAL(20, 10), sigma_ma DECIMAL(30, 10),
	ad DECIMAL(30, 10), sigma_ad DECIMAL(30, 10),
	n DECIMAL(20, 10), sigma_n DECIMAL(30, 10),
	tp DATETIME2(7), sigma_tp DECIMAL(30, 10),
	per DECIMAL(30, 10), sigma_per DECIMAL(30, 10),
	moid DECIMAL(20, 10),
	moid_ld DECIMAL(20, 10),
	rms DECIMAL(10, 5),
	uncertainty VARCHAR(10),
	Reference VARCHAR(50),
	Num_Obs INT,
	Num_Opp INT,
	Arc VARCHAR(20),
	Coarse_Perts VARCHAR(20),
	Precise_Perts VARCHAR(20),
	Hex_Flags VARCHAR(10),
	Is1kmNEO BIT,
	IsCriticalList BIT,
	IsOneOppositionEarlier BIT,
	IDClasse INT,
	FOREIGN KEY (IDAsteroide) REFERENCES Asteroide(IDAsteroide),
	FOREIGN KEY (IDClasse) REFERENCES Classe(IDClasse)
);
GO

CREATE UNIQUE INDEX UQ_Orbita_Asteroide_Epoch ON Orbita(IDAsteroide, epoch) WITH (IGNORE_DUP_KEY = ON);
GO

CREATE TABLE Observacao (
	IDObservacao INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	IDAsteroide INT NOT NULL,
	IDAstronomo INT,
	IDSoftware INT,
	IDEquipamento INT,
	Data_atualizacao DATE,
	Hora TIME,
	Duracao DECIMAL(10, 2),
	Modo VARCHAR(50),
	FOREIGN KEY (IDAsteroide) REFERENCES Asteroide(IDAsteroide),
	FOREIGN KEY (IDAstronomo) REFERENCES Astronomo(IDAstronomo),
	FOREIGN KEY (IDEquipamento) REFERENCES Equipamento(IDEquipamento),
	FOREIGN KEY (IDSoftware) REFERENCES Software(IDSoftware)
);
GO

CREATE TABLE Imagem (
	ID_Imagem INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	IDAsteroide INT NOT NULL,
	URL VARCHAR(255),
	Data_imagem DATE,
	Descricao VARCHAR(255),
	FOREIGN KEY (IDAsteroide) REFERENCES Asteroide(IDAsteroide)
);
GO

CREATE TABLE Alerta (
	ID_Alerta INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	IDAsteroide INT NOT NULL,
	Data_Alerta DATE,
	Prioridade VARCHAR(20),
	Nivel INT,
	Descricao VARCHAR(255),
	Estado VARCHAR(20),
	FOREIGN KEY (IDAsteroide) REFERENCES Asteroide(IDAsteroide)
);
GO