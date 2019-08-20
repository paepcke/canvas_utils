DROP TABLE IF EXISTS WikiPages;
CREATE TABLE WikiPages (
    wiki_page_id bigint,
    wiki_id bigint,
    course_id bigint,
    course_name text COLLATE utf8mb4_unicode_ci,
    wiki_page_title text COLLATE utf8mb4_unicode_ci,
    view_count int,
    editing_roles varchar(60)
) engine=MyIsam;


# Fill in WikiPages:

INSERT INTO canvasdata_aux.WikiPages
SELECT
    wiki_page_id,
    wiki_id,
    parent_course_id AS course_id,
    NULL AS course_name,
    title AS wiki_page_title,
    view_count,
    editing_roles
  FROM
   canvasdata_prd.wiki_page_fact LEFT JOIN canvasdata_prd.wiki_page_dim
    ON wiki_page_fact.wiki_page_id = wiki_page_dim.id
   WHERE workflow_state = 'active';

# Do course_name

UPDATE canvasdata_aux.WikiPages
  LEFT JOIN canvasdata_aux.Courses
    ON canvasdata_aux.WikiPages.course_id  = canvasdata_aux.Courses.course_id
  SET canvasdata_aux.WikiPages.course_name = canvasdata_aux.Courses.course_name;
