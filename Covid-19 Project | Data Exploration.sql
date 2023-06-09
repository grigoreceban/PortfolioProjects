/*

Covid 19 Data Exploration 
Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types

*/



-- Create tables and import data from csv files

CREATE TABLE covid_deaths (
	iso_code VARCHAR(64),
    continent VARCHAR(64),
    location VARCHAR(64),
    dateID DATE,
    population BIGINT,
    total_cases BIGINT,
    new_cases INT,
    new_cases_smoothed DOUBLE,
    total_deaths INT,
    new_deaths INT,
    new_deaths_smoothed DOUBLE,
    total_cases_per_million DOUBLE,
    new_cases_per_million DOUBLE,
    new_cases_smoothed_per_million DOUBLE,
    total_deaths_per_million DOUBLE,
    new_deaths_per_million DOUBLE,
    new_deaths_smoothed_per_million DOUBLE,
    reproduction_rate DOUBLE,
    icu_patients INT,
    icu_patients_per_million DOUBLE,
    hosp_patients INT,
    hosp_patients_per_million DOUBLE,
    weekly_icu_admissions INT,
    weekly_icu_admissions_per_million DOUBLE,
    weekly_hosp_admissions INT,
    weekly_hosp_admissions_per_million DOUBLE
);

LOAD DATA LOCAL INFILE '/usr/local/mysql-8.0.32-macos13-arm64/data/Covid19Project/CovidDeaths.csv' INTO TABLE covid_deaths
FIELDS TERMINATED BY ';'
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

CREATE TABLE covid_vaccinations (
	iso_code VARCHAR(64),
	continent VARCHAR(64),
	location VARCHAR(64),
	dateID DATE,
	total_tests BIGINT,
	new_tests INT,
	total_tests_per_thousand DOUBLE,
	new_tests_per_thousand DOUBLE,
	new_tests_smoothed INT,
	new_tests_smoothed_per_thousand DOUBLE,
	positive_rate DOUBLE,
	tests_per_case DOUBLE,
	tests_units VARCHAR(64),
	total_vaccinations BIGINT,
	people_vaccinated BIGINT,
	people_fully_vaccinated BIGINT,
	total_boosters BIGINT,
	new_vaccinations INT,
	new_vaccinations_smoothed INT,
	total_vaccinations_per_hundred DOUBLE,
	people_vaccinated_per_hundred DOUBLE,
	people_fully_vaccinated_per_hundred DOUBLE,
	total_boosters_per_hundred DOUBLE,
	new_vaccinations_smoothed_per_million INT,
	new_people_vaccinated_smoothed INT,
	new_people_vaccinated_smoothed_per_hundred DOUBLE,
	stringency_index DOUBLE,
	population_density DOUBLE,
	median_age DOUBLE,
	aged_65_older DOUBLE,
	aged_70_older DOUBLE,
	gdp_per_capita DOUBLE,
	extreme_poverty DOUBLE,
	cardiovasc_death_rate DOUBLE,
	diabetes_prevalence DOUBLE,
	female_smokers DOUBLE,
	male_smokers DOUBLE,
	handwashing_facilities DOUBLE,
	hospital_beds_per_thousand DOUBLE,
	life_expectancy DOUBLE,
	human_development_index DOUBLE,
	excess_mortality_cumulative_absolute DOUBLE,
	excess_mortality_cumulative DOUBLE,
	excess_mortality DOUBLE,
	excess_mortality_cumulative_per_million DOUBLE
);

LOAD DATA LOCAL INFILE '/usr/local/mysql-8.0.32-macos13-arm64/data/Covid19Project/CovidVaccinations.csv ' INTO TABLE covid_vaccinations
FIELDS TERMINATED BY ';'
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;



-- Select Data that we are going to be starting with

SELECT location, dateID, total_cases, new_cases, total_deaths, population
FROM Covid19Project.covid_deaths
WHERE continent <> ''
ORDER BY 1, 2;



-- Total Cases vs Total Deaths
-- Shows likelihood of dying if you contract covid in your country

SELECT location, dateID, total_cases, total_deaths, (total_deaths / total_cases) * 100 AS death_rate
FROM Covid19Project.covid_deaths
WHERE continent <> ''
ORDER BY 1, 2;		



-- Total Cases vs Population
-- Shows what percentage of population infected with Covid

SELECT location, dateID, total_cases, population, (total_cases / population) * 100 AS infection_rate
FROM Covid19Project.covid_deaths
WHERE continent <> ''
ORDER BY 1, 2;



-- Countries with Highest Infection Rate compared to Population
-- QUERY #3 for Tableau visualization

SELECT location, population, MAX(total_cases) AS highest_infection_count, MAX((total_cases / population)) * 100 AS infection_rate
FROM Covid19Project.covid_deaths
WHERE continent <> ''
GROUP BY location, population
ORDER BY 4 DESC;



-- Countries with Highest Infection Rate per Day

SELECT location, population, dateID, MAX(total_cases) AS highest_infection_count, MAX((total_cases / population)) * 100 AS infection_rate
FROM Covid19Project.covid_deaths
WHERE continent <> ''
GROUP BY location, population, dateID
ORDER BY 4 DESC;



-- Countries with Highest Death Rate per Population

SELECT location, population, MAX(total_deaths) AS highest_death_count, MAX((total_deaths / population)) * 100 AS death_rate
FROM Covid19Project.covid_deaths
WHERE continent <> ''
GROUP BY location, population
ORDER BY 4 DESC;



-- Countries with Highest Death Count per Population

SELECT location, MAX(total_deaths) AS highest_death_count
FROM Covid19Project.covid_deaths
WHERE continent <> ''
GROUP BY location
ORDER BY 2 DESC;



-- Contintents with Highest Death Count per Population
-- QUERY #2 for Tableau visualization

SELECT continent, SUM(new_deaths) AS total_deaths
FROM Covid19Project.covid_deaths
WHERE continent <> ''
GROUP BY continent
ORDER BY 2 DESC;



-- Global Numbers
-- Shows total cases, total deaths and death rate
-- QUERY #1 for Tableau visualization

SELECT SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, (SUM(new_deaths) / SUM(new_cases)) * 100 AS death_rate
FROM Covid19Project.covid_deaths
WHERE continent <> ''
ORDER BY 1;



-- Total Population vs Vaccinations
-- Shows Percentage of Population that has recieved at least one Covid Vaccine

SELECT dea.continent, dea.location, dea.dateID, dea.population, vac.new_vaccinations, SUM(vac.new_vaccinations) OVER (PARTITION BY
	dea.location ORDER BY dea.location, dea.dateID) AS total_people_vaccinated
FROM Covid19Project.covid_deaths dea
JOIN Covid19Project.covid_vaccinations vac
	ON dea.location = vac.location
    AND dea.dateID = vac.dateID
WHERE dea.continent <> ''
ORDER BY 2, 3;



-- Using CTE to perform Calculation on Partition By in previous query

WITH PercentageVaccinated (continent, location, dateID, population, new_vaccinations, total_people_vaccinated)
AS
(
SELECT dea.continent, dea.location, dea.dateID, dea.population, vac.new_vaccinations, SUM(vac.new_vaccinations) OVER (PARTITION BY
	dea.location ORDER BY dea.location, dea.dateID) AS total_people_vaccinated
FROM Covid19Project.covid_deaths dea
JOIN Covid19Project.covid_vaccinations vac
	ON dea.location = vac.location
    AND dea.dateID = vac.dateID
WHERE dea.continent <> ''
)
SELECT *, (total_people_vaccinated / population) * 100 AS vaccination_rate
FROM PercentageVaccinated;



-- Using Temp Table to perform Calculation on Partition By in previous query

DROP TABLE IF EXISTS PercentageVaccinated;
CREATE TEMPORARY TABLE PercentageVaccinated 
(
	continent VARCHAR(64),
    location VARCHAR(64),
    dateID DATE,
    population BIGINT,
    new_vaccinations BIGINT,
    total_people_vaccinated BIGINT
);
INSERT INTO PercentageVaccinated
SELECT dea.continent, dea.location, dea.dateID, dea.population, vac.new_vaccinations, SUM(vac.new_vaccinations) OVER (PARTITION BY
	dea.location ORDER BY dea.location, dea.dateID) AS total_people_vaccinated
FROM Covid19Project.covid_deaths dea
JOIN Covid19Project.covid_vaccinations vac
	ON dea.location = vac.location
    AND dea.dateID = vac.dateID
WHERE dea.continent <> '';

SELECT *, (total_people_vaccinated / population) * 100 AS vaccination_rate
FROM PercentageVaccinated;



-- Total Population vs Total Vaccinations per Country
-- Total vaccinations includes booster doses

SELECT vac.location, dea.population, SUM(vac.new_vaccinations) AS total_people_vaccinated
FROM Covid19Project.covid_deaths dea
JOIN Covid19Project.covid_vaccinations vac
	ON dea.location = vac.location
    AND dea.dateID = vac.dateID
WHERE dea.continent <> ''
GROUP BY vac.location, dea.population
ORDER BY 1;



-- Total Population vs Total People Vaccinated per Country
-- Shows people vaccinated at least once
-- QUERY #4 for Tableau visualization

SELECT vac.location, dea.population, MAX(vac.people_vaccinated) AS total_people_vaccinated
FROM Covid19Project.covid_deaths dea
JOIN Covid19Project.covid_vaccinations vac
	ON dea.location = vac.location
    AND dea.dateID = vac.dateID
WHERE dea.continent <> ''
GROUP BY vac.location, dea.population
ORDER BY 1;



-- Creating View to store data for later visualizations

CREATE VIEW PercentageVaccinated AS
SELECT dea.continent, dea.location, dea.dateID, dea.population, vac.new_vaccinations, SUM(vac.new_vaccinations) OVER (PARTITION BY
	dea.location ORDER BY dea.location, dea.dateID) AS total_people_vaccinated
FROM Covid19Project.covid_deaths dea
JOIN Covid19Project.covid_vaccinations vac
	ON dea.location = vac.location
    AND dea.dateID = vac.dateID
WHERE dea.continent <> '';

SELECT *
FROM PercentageVaccinated;

