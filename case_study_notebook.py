# Databricks notebook source
# Read the source JSON data into a dataframe.
org_df = spark.read.json("/FileStore/tables/ol_cdump.json");

# COMMAND ----------

# Clean the data.
cleansed_df = org_df.where("title is not null AND trim(title) <> '' AND number_of_pages > 20 AND publish_date > 1950")

# COMMAND ----------

# Register a temp view for data querying.
cleansed_df.createOrReplaceTempView("cleansed_ol_cdump")

# COMMAND ----------

%sql
--Select all "Harry Potter" books
select * from cleansed_ol_cdump where title like '%Harry Potter%';

# COMMAND ----------

%sql
-- Get the book with the most pages
Select alternate_names, authors, bio, birth_date, by_statement, contributions, contributors, copyright_date, covers, created, death_date, description, dewey_decimal_class, dewey_number, download_url, edition_name, excerpts, first_publish_date, first_sentence, full_title, fuller_name, genres, ia_box_id, ia_loaded_id, identifiers, isbn_10, isbn_13, isbn_invalid, isbn_odd_length, key, languages, last_modified, latest_revision, lc_classifications, lccn, links, location, name, notes, number_of_pages, ocaid, oclc_number, oclc_numbers, other_titles, pagination, personal_name, photos, physical_dimensions, physical_format, publish_country, publish_date, publish_places, publishers, purchase_url, revision, series, source_records, subject_people, subject_place, subject_places, subject_time, subject_times, subjects, subtitle, table_of_contents, title, title_prefix, type, uri_descriptions, uris, url, website, weight, work_title, work_titles, works from (select *, rank() over(order by number_of_pages desc) as page_wise_rank from cleansed_ol_cdump) tmp
where page_wise_rank=1;

# COMMAND ----------

%sql
-- Find the Top 5 authors with most written books (assuming author in first position in the array, "key" field and each row is a different book)
 WITH first_cte AS (select *, count(*) over(partition by authors[0].key) as total_books_written from cleansed_ol_cdump)
, second_cte AS (select *, dense_rank() over(order by total_books_written desc) as drnk from first_cte)
Select distinct authors[0].key, total_books_written from second_cte where drnk<=5;

# COMMAND ----------

%sql
-- Find the Top 5 genres with most books
WITH first_cte AS (select *, count(*) over(partition by genres) as total_books from cleansed_ol_cdump)
, second_cte AS (select *, dense_rank() over(order by total_books desc) as drnk from first_cte)
Select distinct genres, total_books from second_cte where drnk<=5;

# COMMAND ----------

%sql
-- Get the avg. number of pages
 select avg(number_of_pages) as avg_no_pages from cleansed_ol_cdump;

# COMMAND ----------

%sql
-- Per publish year, get the number of authors that published at least one book
select publish_date, count(distinct authors[0].key) as no_of_authors from cleansed_ol_cdump group by publish_date;
