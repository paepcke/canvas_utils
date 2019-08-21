USE canvasdata_aux;

DROP TABLE IF EXISTS Modules;
CREATE TABLE Modules (
    id bigint,
    course_id bigint,
    course_name text COLLATE utf8mb4_unicode_ci,
    module_name text COLLATE utf8mb4_unicode_ci,
    account_id bigint,            # module_fact
    enrollment_term_id bigint,    # module_fact
    term_name varchar(20),        # Term
    wiki_id bigint,               # wiki_dim
    wiki_title text COLLATE utf8mb4_unicode_ci  # wiki_dim
    ) ENGINE=MyIsam;

USE canvasdata_prd;

CALL createIndexIfNotExists('mod_id_idx', 'module_fact', 'module_id', NULL);

INSERT into canvasdata_aux.Modules
SELECT id,
       module_dim.course_id,
       NULL AS course_name,
       name AS module_name,
       account_id,
       enrollment_term_id,
       NULL AS term_name,
       wiki_id,
       NULL AS wiki_title
  FROM module_dim LEFT JOIN module_fact
     ON module_dim.id = module_fact.module_id
   WHERE workflow_state = 'active';

# Bring in course name:

UPDATE canvasdata_aux.Modules
  LEFT JOIN canvasdata_aux.Courses
   ON Modules.course_id = canvasdata_aux.Courses.course_id
  SET Modules.course_name = canvasdata_aux.Courses.course_name;

# Bring in Wiki:

UPDATE canvasdata_aux.Modules
  LEFT JOIN wiki_dim
   ON Modules.wiki_id = wiki_dim.id
  SET Modules.wiki_title = wiki_dim.title;

# Bring in term_name:

UPDATE canvasdata_aux.Modules
  LEFT JOIN canvasdata_aux.Terms
   ON Modules.enrollment_term_id = canvasdata_aux.Terms.term_id
  SET Modules.term_name = canvasdata_aux.Terms.term_name;

