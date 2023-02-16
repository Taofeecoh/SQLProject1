SELECT * FROM COVIDProject..COVDeaths
WHERE continent IS NOT NULL

/* Total cases to Total deaths (statistics) */
SELECT location, date, total_cases, total_deaths, ((total_deaths/total_cases) * 100) AS DeathPercentage
--COUNT(total_cases) OVER (PARTITION BY new_cases)
FROM COVIDProject..COVDeaths
WHERE continent IS NOT NULL AND location LIKE 'Afr%'
--ORDER BY 1,2

/* Total cases of COV-19 in the population (statistics)
 along with the percent of cases in the whole population */
SELECT location, date, population, total_cases, ((total_cases/population) * 100) AS CasesPercentageInPopulation
FROM COVIDProject..COVDeaths
WHERE location LIKE '%states%'
ORDER BY 1,2

/* Highest no.of cases in population */
SELECT location, population, 
MAX(CAST(total_cases AS INT)) HighestNoOfInfection, MAX((total_cases/population) * 100) AS MaxCasesPercentByPopulation
FROM COVIDProject..COVDeaths
--WHERE location LIKE '%states%'
GROUP BY location, population
ORDER BY MaxCasesPercentByPopulation DESC

/* Countries with the highest death count per population */

SELECT location,
MAX(CAST (total_deaths AS INT)) HighestDeathCount 
--MAX((total_deaths/population) * 100 ) HighestPercentDeathCount
FROM COVIDProject..COVDeaths
--WHERE location LIKE 'Af%'
WHERE continent IS NOT NULL
GROUP BY location 
ORDER BY HighestDeathCount DESC

/* Exploration by continent */
SELECT location, population,
MAX(CAST (total_deaths AS INT)) HighestDeathCount
--MAX((total_deaths/population) * 100 ) HighestPercentDeathCount
FROM COVIDProject..COVDeaths
--WHERE location LIKE 'Af%'
WHERE continent IS NULL
GROUP BY location, population
ORDER BY HighestDeathCount DESC

/* Exploration of highest death in continent per population */
SELECT location,
MAX(CAST (total_deaths AS INT)) HighestDeathCount, MAX(total_deaths/population * 100) HighestDeathPercent
FROM COVIDProject..COVDeaths
WHERE continent IS NULL
GROUP BY location
ORDER BY HighestDeathCount DESC

/* Continents with highest death count per population */
SELECT continent,
MAX(CAST (total_deaths AS INT)) HighestDeathCount, MAX((total_deaths/population) * 100) HighestDeathPercent
FROM COVIDProject..COVDeaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY HighestDeathCount DESC


/* Statistics of daily vaccinated across continents(location) */
SELECT date,
SUM(new_cases) DailyCases, 
SUM(CONVERT(INT,new_deaths)) DailyDeaths, 
(SUM(CAST(new_deaths AS INT))/SUM(new_cases) * 100) DailyDeathPercent
FROM COVIDProject..COVDeaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY DailyDeathPercent DESC


/* Statistics of daily vaccinated across continents(location) Grouped by( roll over aggregate partitioned by location) */
SELECT Dt.continent, Dt.location, Dt.date, Dt.population, Vc.new_vaccinations,
SUM(CAST(Vc.new_vaccinations AS INT)) OVER (PARTITION BY Dt.location ORDER BY Dt.location, Dt.date) AggDailyVac
FROM COVIDProject..COVDeaths Dt
JOIN COVIDProject..CoVac Vc
	ON Dt.location = Vc.location 
	AND Dt.date = Vc.date
WHERE Dt.continent IS NOT NULL
ORDER BY 2,3

--'WITH' CTE to get percentage of daily vaccinated using the new column 'AggDailyVac'

WITH CTE_Covid(Continent, location, date, New_vaccinations, population, AggDailyVac) AS
(
	SELECT Dt.continent, Dt.location, Dt.date, Vc.new_vaccinations, Dt.population,
SUM(CAST(Vc.new_vaccinations AS INT)) OVER (PARTITION BY Dt.location ORDER BY Dt.location, Dt.date) AggDailyVac
FROM COVIDProject..COVDeaths Dt
JOIN COVIDProject..CoVac Vc
	ON Dt.location = Vc.location 
	AND Dt.date = Vc.date
WHERE Dt.continent IS NOT NULL
--ORDER BY 2,3  
)

SELECT *, (AggDailyVac/population) * 100 PercentDailyVacPerPopulation
FROM CTE_Covid


-- With TEMP TABLE '#' to get percentage of daily vaccinated using the new column 'AggDailyVac'
DROP TABLE IF EXISTS #TempCovidTable
CREATE TABLE #TempCovidTable(
Continent NVARCHAR(255),
Location NVARCHAR(255),
Date DATETIME,
Population NUMERIC,
New_Vaccinations NUMERIC,
AggDailyVac NUMERIC,
)
INSERT INTO #TempCovidTable
SELECT Dt.continent, Dt.location, Dt.date, Dt.population, Vc.new_vaccinations,
SUM(CAST(Vc.new_vaccinations AS INT)) OVER (PARTITION BY Dt.location ORDER BY Dt.location, Dt.date) AggDailyVac
FROM COVIDProject..COVDeaths Dt
JOIN COVIDProject..CoVac Vc
	ON Dt.location = Vc.location 
	AND Dt.date = Vc.date
WHERE Dt.continent IS NOT NULL
--ORDER BY 2,3

SELECT *, (AggDailyVac/population) * 100 PercentDailyVacPerPopulation
FROM #TempCovidTable


--This is a view of the manipulated data ('WITH' &/OR '#') to store for later use
CREATE VIEW CovidDataView AS 
 SELECT Dt.continent, Dt.location, Dt.date, Vc.new_vaccinations, Dt.population,
SUM(CAST(Vc.new_vaccinations AS INT)) OVER (PARTITION BY Dt.location ORDER BY Dt.location, Dt.date) AggDailyVac
FROM COVIDProject..COVDeaths Dt
JOIN COVIDProject..CoVac Vc
	ON Dt.location = Vc.location 
	AND Dt.date = Vc.date
WHERE Dt.continent IS NOT NULL
--ORDER BY 2,3
