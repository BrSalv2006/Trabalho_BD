IF OBJECT_ID('dbo.sp_AlterarEstadoAlerta', 'P') IS NOT NULL DROP PROCEDURE sp_AlterarEstadoAlerta;

IF OBJECT_ID('vw_Last5Detected', 'V') IS NOT NULL DROP VIEW vw_Last5Detected;
IF OBJECT_ID('vw_PHA_NEO', 'V') IS NOT NULL DROP VIEW vw_PHA_NEO;
IF OBJECT_ID('vw_TopCenters', 'V') IS NOT NULL DROP VIEW vw_TopCenters;
IF OBJECT_ID('vw_LargestPHAs', 'V') IS NOT NULL DROP VIEW vw_LargestPHAs;
IF OBJECT_ID('vw_EstatisticasAlerta', 'V') IS NOT NULL DROP VIEW vw_EstatisticasAlerta;
IF OBJECT_ID('vw_ProximosEventosCriticos', 'V') IS NOT NULL DROP VIEW vw_ProximosEventosCriticos;
IF OBJECT_ID('vw_EstatisticasDescoberta', 'V') IS NOT NULL DROP VIEW vw_EstatisticasDescoberta;
IF OBJECT_ID('vw_EvolucaoPrecisao', 'V') IS NOT NULL DROP VIEW vw_EvolucaoPrecisao;

IF OBJECT_ID('trg_alerta_alta_prioridade', 'TR') IS NOT NULL DROP TRIGGER trg_alerta_alta_prioridade;
IF OBJECT_ID('trg_alerta_media_prioridade', 'TR') IS NOT NULL DROP TRIGGER trg_alerta_media_prioridade;
IF OBJECT_ID('trg_alerta_baixa_prioridade', 'TR') IS NOT NULL DROP TRIGGER trg_alerta_baixa_prioridade;

IF OBJECT_ID('Alerta', 'U') IS NOT NULL DROP TABLE Alerta;
IF OBJECT_ID('Imagem', 'U') IS NOT NULL DROP TABLE Imagem;
IF OBJECT_ID('Observacao', 'U') IS NOT NULL DROP TABLE Observacao;
IF OBJECT_ID('Orbita', 'U') IS NOT NULL DROP TABLE Orbita;
IF OBJECT_ID('Equipamento', 'U') IS NOT NULL DROP TABLE Equipamento;
IF OBJECT_ID('Astronomo', 'U') IS NOT NULL DROP TABLE Astronomo;
IF OBJECT_ID('Asteroide', 'U') IS NOT NULL DROP TABLE Asteroide;
IF OBJECT_ID('Centro_de_observacao', 'U') IS NOT NULL DROP TABLE Centro_de_observacao;
IF OBJECT_ID('Software', 'U') IS NOT NULL DROP TABLE Software;
IF OBJECT_ID('Classe', 'U') IS NOT NULL DROP TABLE Classe;