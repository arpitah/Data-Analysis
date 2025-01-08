
use layoffs;


-- created a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
create table layoffs_staging
like layoffs;

select * from layoffs_staging;

insert into layoffs_staging
select * from layoffs;


--First check for the duplicates and remove duplicates
-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially
select * from layoffs_staging;

with duplicate_cte as(
select *,
row_number() over(partition by country,funds_raised_millions,stage,industry,total_laid_off,percentage_laid_off,'date',location) as row_num
from layoffs_staging
) 
  
Delete from duplicate_cte
where row_num>1;



select * from layoffs_staging
where company='Casper';

--create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(partition by country,funds_raised_millions,stage,industry,total_laid_off,percentage_laid_off,'date',location) as row_num
from layoffs_staging;


select * from layoffs_staging2;


Delete from layoffs_staging2
where row_num >1;

select * from layoffs_staging2
where row_num>1;


-- 2. Standardize Data

SELECT * 
FROM layoffs_staging2;

-- if we look at industry it looks like we have some null and empty rows
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- let's take a look at these
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';
-- nothing wrong here
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------

-- Crypto has multiple different variations. We need to standardize that 
-- consolidated variations of "Crypto" into a single value
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- now that's taken care of:
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

-- --------------------------------------------------
-- we also need to look at 

SELECT *
FROM layoffs_staging2;

-- we have some "United States" and some "United States." with a period at the end. Let's standardize this.
--converted variations of "United States"
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;


-- Let's also fix the date columns:
--date column is having a text datatype change this into a date format
SELECT *
FROM layoffs_staging2;

-- we can use str to date to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(date, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN date DATE;


SELECT *
FROM layoffs_staging2;





-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that

-- so there isn't anything I want to change with the null values

-- 4. remove any columns and rows we need to

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM layoffs_staging2;
