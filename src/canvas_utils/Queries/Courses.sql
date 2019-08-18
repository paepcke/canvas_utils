# By bringing in 'claimed' courses, we are pulling 'bad' data
# in that unpublished. So they do not include all data normally
# available for published courses. We want the courses represented
# anyway, because number of 'claimed' courses is sometimes requested.


USE canvasdata_aux;

DROP TABLE IF EXISTS Courses;
CREATE TABLE Courses (
    account_id bigint(20),
    course_id bigint(20),
    enrollment_term_id bigint(20),
    term_name varchar(60),
    account_name varchar(255),          # German Language (GERLANG)
    course_name varchar(255),           # I Fly Away
    stanford_course_id int,             # 2455342
    code varchar(255),                  # W19-MUSIC-276C-0
    date_end datetime,                  # 2019-04-01 00:00:00
    quarter_name_canvas varchar(16),    # Fall 2015, Default Term, Migrated Content
    quarter_name_peoplesoft varchar(40),# Fall, Summer, not given this year
    acad_year varchar(25),              # 2018-2019
    course_code varchar(30),            # AA110
    subject varchar(15),                # AA
    units_max int,                      # 3
    ger_fulfillment varchar(255),       # THINK,WAY-ER,Writing 1
    acad_group varchar(25),             # ENGR
    department varchar(40),             # AEROASTRO
    acad_career varchar(10),            # UG
    instructors varchar(255),           # 'Jane Doe', 'Jane Doe, Ken Franklin'
    enrollment int,
    workflow_state varchar(40)
    ) engine=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    
# <end_creation>


INSERT INTO Courses
SELECT account_id,
       course_dim.id AS course_id,
       enrollment_term_id,
       NULL AS term_name,
       NULL AS account_name,
       course_dim.name AS course_name,
       NULL AS stanford_course_id,
       code,
       NULL AS date_end,
       NULL AS quarter_name_canvas,
       NULL AS quarter_name_peoplesoft,
       NULL AS acad_year,
       NULL AS course_code,
       NULL AS subject,
       NULL AS units_max,
       NULL AS ger_fulfillment,
       NULL AS acad_group,
       NULL AS department,
       NULL AS acad_career,
       NULL AS instructors,
       NULL AS enrollment,
       course_dim.workflow_state
 FROM canvasdata_prd.account_dim LEFT JOIN canvasdata_prd.course_dim
   ON canvasdata_prd.account_dim.id = canvasdata_prd.course_dim.account_id
   WHERE canvasdata_prd.course_dim.workflow_state IN ('claimed', 'available');

# Fill quarter_name (e.g. Fall 2015, Summer 2016),
# and date_end:

UPDATE Courses
  LEFT JOIN canvasdata_prd.enrollment_term_dim
   ON Courses.enrollment_term_id = id
  LEFT JOIN Terms ON canvasdata_prd.enrollment_term_dim.id = Terms.term_id
  SET quarter_name_canvas = name,
      Courses.date_end    = enrollment_term_dim.date_end
 WHERE Terms.term_name NOT IN ('Tester Term', 'Migrated Content', 'Default Term', 'Test Term');

# Fill in account_name:

UPDATE Courses
  LEFT JOIN canvasdata_prd.account_dim
   ON account_id = id
  SET account_name = name;

# All else from ExploreCourses.

# Add enrollment:

UPDATE Courses
  LEFT JOIN
       (SELECT course_id,
              COUNT(course_id) AS enrollment
         FROM Courses LEFT JOIN canvasdata_prd.enrollment_dim
            USING(course_id)
          GROUP BY course_id
       ) AS EnrlRes
   USING(course_id)
 SET Courses.enrollment = EnrlRes.enrollment;

# Now delete courses in which the canvas quarter
# does not match the peoplesoft quarter. Example
# canvas quarter value: "Winter 2015". An
# example of peoplesoft quarter_name is just "Winter"
# or "Fall".
# 
# Those mismatches indicate badly matched Canvas
# course offerings with Peoplesoft courses.
# 
# The following lowers the number of courses in Courses by
# about 30% (e.g. 96,448 to 64,304)

DELETE FROM Courses
  WHERE quarter_name_canvas IS NOT NULL
    and quarter_name_peoplesoft IS NOT NULL
    and quarter_name_peoplesoft != substring_index(quarter_name_canvas, ' ',1);

CREATE INDEX crs_nm_idx ON Courses(course_name(50));
CREATE INDEX crs_id_idx ON Courses(course_id);

# Add term name:

UPDATE Courses
  LEFT JOIN Terms
    ON Courses.enrollment_term_id = Terms.term_id
  SET Courses.term_name = Terms.term_name
  WHERE Terms.term_name NOT IN ('Tester Term', 'Migrated Content', 'Default Term', 'Test Term');
