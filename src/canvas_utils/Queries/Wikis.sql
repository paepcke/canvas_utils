USE canvasdata_aux;

DROP TABLE IF EXISTS Wikis;
CREATE TABLE Wikis (
    wiki_id bigint,
    account_id bigint,
    course_id bigint,
    course_name text COLLATE utf8mb4_unicode_ci,
    enrollment_term_id bigint,
    term_name varchar(30),      # get
    wiki_title text COLLATE utf8mb4_unicode_ci,
    wiki_type varchar(10)
) engine=MyIsam;

# Get Wiki entries:

USE canvasdata_prd;

INSERT INTO canvasdata_aux.Wikis
SELECT 
    wiki_id,
    account_id,
    parent_course_id AS course_id,
    NULL AS course_name,
    enrollment_term_id,
    NULL AS term_name,
    title AS wiki_title,
    parent_type AS wiki_type 
  FROM wiki_fact LEFT JOIN wiki_dim
    ON wiki_fact.wiki_id = wiki_dim.id;

# Get course_name:

UPDATE canvasdata_aux.Wikis
  LEFT JOIN canvasdata_aux.Courses
    ON canvasdata_aux.Wikis.course_id  = canvasdata_aux.Courses.course_id
  SET canvasdata_aux.Wikis.course_name = canvasdata_aux.Courses.course_name;

# Get Term name
UPDATE canvasdata_aux.Wikis
  LEFT JOIN canvasdata_aux.Terms
    ON canvasdata_aux.Wikis.enrollment_term_id  = canvasdata_aux.Terms.term_id
  SET canvasdata_aux.Wikis.term_name = canvasdata_aux.Terms.term_name;


