-- Data Cleaning using MySQL Project

-- Data Source - https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- I got an overview of the data and studied each column
SELECT *
FROM layoffs;

/*  I created a staging table of the data. Here is why :

- Staging tables enhance data quality by allowing thorough cleansing and 
validation before transferring data to final tables, ensuring accuracy and reliability.
- They improve system performance by enabling batch processing, 
which reduces the strain on primary databases and speeds up data integration processes. */

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

/* When cleaning data, I usually follow a few steps:

1. Identify and remove duplicates
2. Standardize data formats
3. Handle missing or null values
4. Remove any columns and rows that are not necessary */


-- 1. Identify and remove duplicates 

SELECT *
FROM layoffs_staging;

-- I'm using row number function to get the upicates because it assigns a unique row number to each record or row. 

SELECT *, 
		ROW_NUMBER () OVER (
		                 PARTITION BY company, location, industry,  percentage_laid_off, `date`)
						 AS row_num
FROM layoffs_staging;

SELECT * 
FROM (
       SELECT *, 
               ROW_NUMBER () OVER (
                         PARTITION BY company, location, industry,  percentage_laid_off, `date`)
						AS row_num
		FROM layoffs_staging
	) AS duplicates 
WHERE row_num > 1;

-- I then go through some companies  to confirm for duplicates 
SELECT *
FROM layoffs_staging
WHERE company = 'Hibob';

-- It seems these entries are legitimate and shouldn't be removed, so we need to carefully review each row to ensure accuracy.

SELECT *
FROM (
	SELECT *,
		   ROW_NUMBER() OVER (
			                PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	 FROM layoffs_staging
     ) duplicates
WHERE row_num > 1;

-- To delete these dupicates,I turn the subquery into  a CTE 
WITH duplicate_cte AS 
(
		 SELECT *
         FROM (
				SELECT *,
				ROW_NUMBER() OVER (
							  PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
				) AS row_num
	            FROM layoffs_staging
                ) duplicates
          WHERE row_num > 1 
)
DELETE 
FROM duplicate_cte;

-- The above query won't work obviously because you can't delete from a cte
-- The solution is to create  a new table like layoffs_staging but with the row number included 

 CREATE TABLE layoffs_staging2 (
   company TEXT,
   location TEXT,
   industry TEXT,
   total_laid_off  INT DEFAULT NULL,
   percentage_laid_off  TEXT,
   `date` TEXT,
   stage TEXT,
   country TEXT,
   funds_raised_millions INT DEFAULT NULL,
  row_num INT
);

INSERT INTO layoffs_staging2
SELECT *,
	   ROW_NUMBER() OVER (
						 PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
				) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- The duplicates can now be easily deleted 

DELETE FROM layoffs_staging2
WHERE row_num > 1;



-- 2. Standardize data format

SELECT *
FROM layoffs_staging2;

-- Looking at the data,some null and empty rows can be seen
-- I'd take industry 

SELECT  *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';
-- Looking at the above query,there aren't other records that can be used to populate the data or the industry column


SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

-- But for companies like'airbnb%', it can be seen that it's in the travel industry
-- So there is a need to populate this data and fill null and emty rows for the industry
-- Bu some rows appear as blank and can't be easily retrieved, so the blank ows will be filled with nulls

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
ORDER BY industry;

-- The null vaues can now be populated
-- To fill the null valuues or rows in industry,a self join is used 
-- A self join helps fill in missing (null) values by matching rows in a table 
-- with other rows in the same table that have the needed information.
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- I then check if it's only company like 'Bally%' hat isn't populated

SELECT  *
FROM layoffs_staging2
WHERE industry IS NULL;

SELECT DISTINCT industry
FROM layoffs_staging2;

-- While reviewing the distinct industries, I noticed variations in how Crypto is listed, which need to be standardized.

SELECT DISTINCT industry 
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- These variations are now taken care of 

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- I confirm 

SELECT DISTINCT industry 
FROM layoffs_staging2;

-- I checked other columns to know other columns that need standardization

SELECT *
FROM layoffs_staging2;

-- The date column needs to be standardized 
SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y') AS updated_date
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Now the date can be converted to date type

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- The data has been standardized



-- 3. Handle missing or null values

-- Records that contain mising or null values in some columns have been attended to but those in total_laid_off and percentage_laid_off
-- and funds_raised_millions because they'd be used in further analysis



-- 4. Remove any columns and rows that are not necessary

SELECT *
FROM layoffs_staging2;

-- I know I stated that total laid off and perecentage laid off should be left 
-- but when they are both null.the record can be useless,so hey need to be deleted

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

-- I confirm

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

-- The row num column created earlier has fulfilled its purpose, so it's time to let it go

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- I confirm 
SELECT *
FROM layoffs_staging2;
