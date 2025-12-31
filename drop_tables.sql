/*
 * Drop Tables Script
 *
 * Drops all objects created by database.sql.
 * Drops Views first, then Tables in reverse dependency order.
 */

-- =============================================
-- 1. Drop Views
-- =============================================

IF OBJECT_ID('vw_Last5Detected', 'V') IS NOT NULL DROP VIEW vw_Last5Detected;
IF OBJECT_ID('vw_PHA_NEO', 'V') IS NOT NULL DROP VIEW vw_PHA_NEO;
IF OBJECT_ID('vw_TopCenters', 'V') IS NOT NULL DROP VIEW vw_TopCenters;
IF OBJECT_ID('vw_LargestPHAs', 'V') IS NOT NULL DROP VIEW vw_LargestPHAs;
IF OBJECT_ID('vw_EstatisticasAlerta', 'V') IS NOT NULL DROP VIEW vw_EstatisticasAlerta;
IF OBJECT_ID('vw_ProximosEventosCriticos', 'V') IS NOT NULL DROP VIEW vw_ProximosEventosCriticos;
IF OBJECT_ID('vw_EstatisticasDescoberta', 'V') IS NOT NULL DROP VIEW vw_EstatisticasDescoberta;
IF OBJECT_ID('vw_EvolucaoPrecisao', 'V') IS NOT NULL DROP VIEW vw_EvolucaoPrecisao;

-- =============================================
-- 2. Drop Tables (Reverse Dependency Order)
-- =============================================

-- Level 3: Tables that depend on Level 2 or Level 1 tables
IF OBJECT_ID('Observacao', 'U') IS NOT NULL DROP TABLE Observacao;
IF OBJECT_ID('Alerta', 'U') IS NOT NULL DROP TABLE Alerta;
IF OBJECT_ID('Imagem', 'U') IS NOT NULL DROP TABLE Imagem;
IF OBJECT_ID('Orbita', 'U') IS NOT NULL DROP TABLE Orbita;

-- Level 2: Tables that depend on Level 1 tables
IF OBJECT_ID('Equipamento', 'U') IS NOT NULL DROP TABLE Equipamento;
IF OBJECT_ID('Astronomo', 'U') IS NOT NULL DROP TABLE Astronomo;
IF OBJECT_ID('Asteroide', 'U') IS NOT NULL DROP TABLE Asteroide;

-- Level 1: Independent Tables
IF OBJECT_ID('Software', 'U') IS NOT NULL DROP TABLE Software;
IF OBJECT_ID('Centro_de_observacao', 'U') IS NOT NULL DROP TABLE Centro_de_observacao;
IF OBJECT_ID('Classe', 'U') IS NOT NULL DROP TABLE Classe;

PRINT 'All database objects dropped successfully.';
