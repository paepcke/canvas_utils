DROP TABLE IF EXISTS ExploreCourses;
CREATE TABLE ExploreCourses (
    course_code varchar(15),      # AA110
    subject varchar(15),          # AA
    course_name varchar(255),     # I Fly Away
    units_min int,                # 1
    units_max int,                # 3
    acad_year varchar(25),        # 2018-2019
    course_id int,                # Peoplesoft course id
    acad_group varchar(25),       # ENGR
    department varchar(40),       # AEROASTRO
    acad_career varchar(10),      # UG
    ger_fulfillment varchar(255), # THINK,WAY-ER,Writing 1
    quarter_name varchar(40),     # 'Fall', 'Summer', 'not given this year'
    instructors varchar(255)      # 'Jane Doe', 'Jane Doe, Ken Franklin'
    ) engine=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

# <end_creation>


# Pick up the Explore Courses parse of the EC .xml file we did
# ahead of loading this file:

LOAD DATA LOCAL INFILE '/Users/paepcke/EclipseWorkspacesNew/canvas_utils/src/canvas_utils/Data/explore_courses.csv'
 INTO TABLE ExploreCourses
  FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n'
  IGNORE 1 LINES;

CREATE INDEX crs_nm_idx ON ExploreCourses(course_name(50));
