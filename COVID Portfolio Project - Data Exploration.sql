-- Data import (same method applies for the "covid_deaths" table --

USE portfolio_project;

CREATE TABLE covid_vaccinations(
	iso_code text,
	continent text,
	location text,	
	date date,
	new_tests integer,
    total_tests_per_thousand float,
    new_tests_per_thousand float,
    new_tests_smoothed integer,
    new_tests_smoothed_per_thousand float,
    positive_rate float,
    tests_per_case float,
    tests_units text,
    total_vaccinations integer,
    people_vaccinated integer,
    people_fully_vaccinated integer,
    total_boosters integer,
    new_vaccinations integer,
    new_vaccinations_smoothed integer,
    total_vaccinations_per_hundred float,
    people_vaccinated_per_hundred float,
    people_fully_vaccinated_per_hundred float,
    total_boosters_per_hundred float,
    new_vaccinations_smoothed_per_million integer,
    new_people_vaccinated_smoothed integer,
    new_people_vaccinated_smoothed_per_hundred float,
    stringency_index float,
    population_density float,
    median_age float,
    aged_65_older float,
    aged_70_older float,
    gdp_per_capita float,
    extreme_poverty	float,
    cardiovasc_death_rate float,
    diabetes_prevalence float,
    female_smokers float,
    male_smokers float,
    handwashing_facilities float,
	hospital_beds_per_thousand float,
    life_expectancy	float,
    human_development_index float,
    excess_mortality_cumulative_absolute float, 	
    excess_mortality_cumulative float,	
    excess_mortality float, 	
    excess_mortality_cumulative_per_million float
);

SET GLOBAL local_infile = 1 

LOAD DATA IN FILE '/Users/oluwadamilolavera-cruz/Downloads/Portfolio/CovidVaccinations.csv'
INTO TABLE covid_vaccinations
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;


/*
Covid 19 Data Exploration 
Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types
*/

SELECT 
	*
FROM
	portfolio_project.covid_deaths
WHERE
	continent != ""
ORDER BY
	2,3
    
-- Select the data that we will be working with --

SELECT 
	location, 
    date, 
    total_cases, 
    new_cases,
    total_deaths, 
    population
FROM
	portfolio_project.covid_deaths
WHERE 
	continent != ""
ORDER BY
	1,2
    
-- Total Cases vs Total Deaths
-- Shows likelihood of dying if you contract covid in UK & Nigeria --

SELECT
	location, 
    date, 
    total_cases, 
	total_deaths, 
    (total_deaths/total_cases)*100 as death_percentage   
FROM
	portfolio_project.covid_deaths
WHERE 
	continent != "" AND
    (location like '%United Kingdom%' or location like '%Nigeria%')
ORDER BY
	1,2
    
-- Total Cases vs Population
-- Shows what percentage of population infected with Covid

SELECT
	location, 
    date, 
    population,
    total_cases, 
    (total_cases/population)*100 as population_percentage_infected
FROM
	portfolio_project.covid_deaths
/* where 
	continent != "" AND
    (location like '%United Kingdom%' or location like '%Nigeria%')*/
ORDER BY
	1,2
    
-- Countries with Highest Infection Rate compared to Population
SELECT 
	location, 
    population,
    MAX(total_cases) as highest_infection_count, 
    MAX((total_cases/population))*100 as max_population_percentage_infected   
FROM
	portfolio_project.covid_deaths
WHERE 
	continent != ""
GROUP BY 
	location, population
ORDER BY
	max_population_percentage_infected desc
    
-- Countries with Highest Death Count per Population

SELECT 
	location, 
    MAX(total_deaths) as total_death_count
FROM
	portfolio_project.covid_deaths
WHERE
	continent != ""
GROUP BY 
	location
ORDER BY
	total_death_count desc
    
-- BREAKING THINGS DOWN BY CONTINENT

-- Showing contintents with the highest death count per population

SELECT 
	continent, 
    MAX(total_deaths) as total_death_count
FROM
	portfolio_project.covid_deaths
WHERE
	continent != ""
GROUP BY 
	continent
ORDER BY
	total_death_count desc

-- GLOBAL NUMBERS PER DAY

SELECT 
	date,
	SUM(new_cases) as total_cases,
    SUM(new_deaths) as total_deaths, 
    SUM(new_deaths)/SUM(new_cases)*100 as death_percentage
FROM
	portfolio_project.covid_deaths
WHERE
	continent != ""
GROUP BY
	date
ORDER BY
	1,2
    
-- GLOBAL NUMBERS TOTAL

SELECT 
	SUM(new_cases) as total_cases,
    SUM(new_deaths) as total_deaths, 
    SUM(new_deaths)/SUM(new_cases)*100 as death_percentage
FROM
	portfolio_project.covid_deaths
WHERE
	continent != ""
ORDER BY
	1,2

-- Covid Death and Covid Vaccinations Table Join

SELECT
	*
FROM
	portfolio_project.covid_deaths dea
JOIN
	portfolio_project.covid_vaccinations vac
	ON dea.location = vac.location AND
    dea.date = vac.date

-- Total Population vs Vaccinations
-- Shows Percentage of Population that has recieved at least one Covid Vaccine

SELECT
	dea.continent, 
    dea.location, 
    dea.date, 
    dea.population, 
    vac.new_vaccinations, 
    SUM(vac.new_vaccinations) OVER 
    (PARTITION BY dea.location ORDER BY dea.location, dea.date) as rolling_people_vaccinated 
FROM
	portfolio_project.covid_deaths dea
JOIN
	portfolio_project.covid_vaccinations vac
	ON dea.location = vac.location AND
    dea.date = vac.date
WHERE
	dea.continent != ""
ORDER BY
	2,3

-- Using CTE to perform Calculation on Partition By in previous query

WITH PopvsVac (continent, location, date, population, new_vaccinations, rolling_people_vaccinated) as
(
SELECT
	dea.continent, 
    dea.location, 
    dea.date, 
    dea.population, 
    vac.new_vaccinations, 
    SUM(vac.new_vaccinations) OVER 
    (PARTITION BY dea.location ORDER BY dea.location, dea.date) as rolling_people_vaccinated 
FROM
	portfolio_project.covid_deaths dea
JOIN
	portfolio_project.covid_vaccinations vac
	ON dea.location = vac.location AND
    dea.date = vac.date
WHERE
	dea.continent != ""
-- ORDER BY 2,3
)
SELECT
	*, 
    (rolling_people_vaccinated/population)*100 as rolling_percentatge_population_vaccinated
FROM
	PopvsVac

-- Creating View to store data for later visualizations

CREATE VIEW Percentage_Population_Vaccinated as
SELECT
	dea.continent, 
    dea.location, 
    dea.date, 
    dea.population, 
    vac.new_vaccinations, 
    SUM(vac.new_vaccinations) OVER 
    (PARTITION BY dea.location ORDER BY dea.location, dea.date) as rolling_people_vaccinated 
FROM
	portfolio_project.covid_deaths dea
JOIN
	portfolio_project.covid_vaccinations vac
	ON dea.location = vac.location AND
    dea.date = vac.date
WHERE
	dea.continent != ""
