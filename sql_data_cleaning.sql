-- DATA CLEANING

select * from layoffs;
select count(*) from layoffs;
# making a duplicate table, this is the one we will work in and clean the data. 
-- We want a table with the raw data in case something happens
create table layoffs_staging
like layoffs;
insert into layoffs_staging
select * from layoffs;

select * from layoffs_staging;
# In Data Cleaning, we usually do following steps-
-- 1. checking for duplicates and removing them if any
-- 2. Standardizing data and fixing errors
-- 3. Looking at Null values
-- 4. To remove any irrelevant rows or columns

# removing duplicates
with duplicate_cte as
(
select *,
row_number() over(partition by company, industry, total_laid_off, percentage_laid_off, `date`) as rowNum
from layoffs_staging
)
select * from duplicate_cte where rownum > 1;

# let's check company = 'Oda'
select * from layoffs_staging where company = 'Oda';
-- we see that Oda company have all legitimate entries, not any duplicate, therefore partition by each column
with duplicate_cte as
(
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) as rowNum
from layoffs_staging
)
select * from duplicate_cte where rownum > 1;

select * from layoffs_staging where company = 'casper'; # taking example
-- we can't directly delete rows from duplicate_cte where rowNum > 1 as update statement in cte is not allowed
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `rowNum` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
insert into layoffs_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) as rowNum
from layoffs_staging;

select * from layoffs_staging2;

delete from layoffs_staging2 where rowNum > 1;   # now deleting duplicates
select * from layoffs_staging2;
# 2. Standardizing data and fixing errors

select distinct(company) from layoffs_staging2;
select company, trim(company) from layoffs_staging2;
UPDATE layoffs_staging2
SET company = trim(company);

select distinct(industry) from layoffs_staging2
order by 1;                                     -- we need to do changes in crypto, cryptoCurrency etc.
select * from layoffs_staging2 where industry like 'Crypto%';
UPDATE layoffs_staging2
SET industry = 'Crypto'
where industry like 'Crypto%';

select distinct location from layoffs_staging2    -- looks fine, having no issue
order by 1; 

select distinct country from layoffs_staging2
order by 1; 
select distinct country, trim(trailing '.' from country)
from layoffs_staging2 order by 1;
UPDATE layoffs_staging2
SET country = trim(trailing '.' from country)
where country like 'United States%'; 

select `date` from layoffs_staging2; 
select `date`,
str_to_date(`date`, '%m/%d/%Y') from layoffs_staging2;
UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');
alter table  layoffs_staging2                  -- changing date to text from DATE
modify column `date` DATE;
# 3........ Looking at Null values........

select * from layoffs_staging2
where industry is Null or industry = '';
-- we try to populate industry if there is more than one row of same company
select * from layoffs_staging2
where company = 'Airbnb';
update layoffs_staging2
set industry = NULL             -- we have 1st set industry = NULL bcz a blank value is not NULL
where industry = '';
select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
ON t1.company = t2.company and t1.location = t2.location
where (t1.industry is NULL)
and t2.industry is not NULL;
update layoffs_staging2 t1
join layoffs_staging2 t2 
	ON t1.company = t2.company and t1.location = t2.location
set t1.industry = t2.industry
where  (t1.industry is NULL)
and t2.industry is not NULL;
# Now if run again query at line 101, industry has populated
select * from layoffs_staging2
where company like 'Bally%';
 
# 4. .........To remove any irrelevant rows or columns........
select * from layoffs_staging2
where total_laid_off is NULL and percentage_laid_off is NULL;
-- the data is of no use if total_laid_off and percentage_laid_off are both NULL
delete from layoffs_staging2
where total_laid_off is NULL and percentage_laid_off is NULL;
# to delete column
alter table layoffs_staging2
drop column rowNum;

# .......... Exploratory Data Analysis ................

# Below query is useful for getting the top 5 records with the highest total_laid_off values for individual rows.
SELECT company, total_laid_off
FROM layoffs_staging
ORDER BY 2 DESC LIMIT 5;
# Below query is useful for finding the top 5 companies with the highest total layoffs across all their records.
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC LIMIT 10;

# to get the companies with the most layoffs and by year
with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
),
Company_year_rank as
(
select *,
dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year where years is not NULL
)
select * from company_year_rank
where ranking <= 5;
# ..... rolling total of layoffs per month ......
WITH ROLLING_TOTAL AS 
(
SELECT SUBSTRING(`date`,1,7) as `month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
where SUBSTRING(`date`,1,7) is not NULL
GROUP BY `month`
ORDER BY 1 
)
SELECT `month`, total_off, SUM(total_off) OVER (ORDER BY `month` ASC) as rolling_total_layoffs
FROM ROLLING_TOTAL
ORDER BY `month`;


