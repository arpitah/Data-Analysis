#Data cleaning
use layoffs;
create table layoffs_staging
like layoffs;


# Standardize the data
#Remove Duplicates
#Null Values or blank values
#remove any columns
select * from layoffs_staging;

insert into layoffs_staging
select * from layoffs;


with duplicate_cte as(
select *,
row_number() over(partition by country,funds_raised_millions,stage,industry,total_laid_off,percentage_laid_off,'date',location) as row_num
from layoffs_staging
) 
Delete from duplicate_cte
where row_num>1;

select * from layoffs_staging
where company='Casper';


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


#standardizing data
update layoffs_staging2
set company=TRIM(company);

select distinct industry
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry like '%Crypto%';

update layoffs_staging2
set industry='Crypto'
where industry like '%Crypto%';

select distinct country,TRIM(TRAILING '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country=TRIM(TRAILING '.' from country)
where country like 'United States%';

select date from layoffs_staging2
order by 1;

select date,
STR_TO_DATE(date,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set date=STR_TO_DATE(date,'%m/%d/%Y');

Alter table layoffs_staging2
modify column date Date;

select *
from layoffs_staging2
where total_laid_off is null;

select*
from layoffs_staging2
where industry is null
or industry='';

select*
from layoffs_staging2
where percentage_laid_off is null;

select*
from layoffs_staging2
where company='Airbnb';

select*
from layoffs_staging2 stg21
join layoffs_staging2 stg22
on stg21.company=stg22.company
and stg21.location=stg22.location
where stg21.industry is null 
and stg22.industry is not null;


update layoffs_staging2
set industry=NULL
WHERE industry='';


update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
set t1.industry=t2.industry
where (t1.industry is null or t1.industry='')
and t2.industry is not null;


select * from layoffs_staging2
order by company;


select * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

delete from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

















