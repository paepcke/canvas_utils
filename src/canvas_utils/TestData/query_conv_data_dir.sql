LOAD DATA LOCAL INFILE '<data_dir>/explore_courses.csv' INTO TABLE
 ExploreCourses FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY '"'
 LINES TERMINATED BY '\n' IGNORE 1 LINES;