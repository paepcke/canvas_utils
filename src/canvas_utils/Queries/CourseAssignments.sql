USE canvasdata_aux;

DROP TABLE IF EXISTS CourseAssignments;
CREATE TABLE CourseAssignments (
    account_id bigint,
    course_id bigint,
    course_name varchar(255),
    term_name varchar(60),
    assignment_id bigint,
    assignment_group_id bigint,
    assignment_name varchar(255),
    submission_types varchar(255),
    points_possible double,
    grading_type varchar(255),
    due_at timestamp,
    quiz_id bigint,
    quiz_type varchar(30),
    quiz_name varchar(255),
    quiz_assignment_id bigint,
    quiz_time_limit int,
    quiz_allowed_attempts int,
    group_assignment_name varchar(255),
    group_assignment_weight double,
    current_score double,
    final_score double
    ) engine=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

# <end_creation>


INSERT INTO CourseAssignments
SELECT
    NULL AS account_id,                    # canvasdata_prd.assignment_group_score_fact.account_id,
    canvasdata_prd.assignment_dim.course_id,
    NULL AS course_name,
    NULL AS term_name,
    canvasdata_prd.assignment_dim.id as assignment_id,
    canvasdata_prd.assignment_dim.assignment_group_id,
    canvasdata_prd.assignment_dim.title as assignment_name,
    canvasdata_prd.assignment_dim.submission_types,
    canvasdata_prd.assignment_dim.points_possible,
    canvasdata_prd.assignment_dim.grading_type,
    NULL AS due_at,
    NULL AS quiz_id,
    NULL AS quiz_type,
    NULL AS quiz_name,
    NULL AS quiz_assignment_id,
    NULL AS quiz_time_limit,
    NULL AS quiz_allowed_attempts,
    canvasdata_prd.assignment_group_dim.name as group_assignment_name,
    NULL AS group_assignment_weight,        #  canvasdata_prd.assignment_group_fact.group_weight as group_assignment_weight,
    NULL AS current_score, # canvasdata_prd.assignment_group_score_fact.current_score as current_score,
    NULL AS final_score    # canvasdata_prd.assignment_group_score_fact.final_score as final_score
FROM canvasdata_prd.assignment_dim
    LEFT JOIN canvasdata_prd.assignment_group_dim
       ON canvasdata_prd.assignment_dim.assignment_group_id = canvasdata_prd.assignment_group_dim.id
     WHERE canvasdata_prd.assignment_dim.course_id           = canvasdata_prd.assignment_group_dim.course_id
       AND canvasdata_prd.assignment_dim.workflow_state      = 'published';

CREATE INDEX crs_id_idx ON CourseAssignments(course_id);

USE canvasdata_prd;

CALL createIndexIfNotExists('id_idx', 'quiz_fact', 'quiz_id', NULL);
CALL createIndexIfNotExists('course_id_idx', 'quiz_fact', 'course_id', NULL);

CREATE TEMPORARY TABLE IF NOT EXISTS QuizDimFact
SELECT quiz_dim.id,
       quiz_dim.quiz_type,
       quiz_dim.name,
       quiz_dim.due_at,
       quiz_dim.assignment_id,
       quiz_fact.points_possible,
       quiz_fact.time_limit,
       quiz_fact.allowed_attempts,
       quiz_fact.course_id
  FROM quiz_dim LEFT JOIN quiz_fact
    ON id = quiz_id
 WHERE quiz_dim.workflow_state = 'published';


CALL createIndexIfNotExists('crs_id_idx', 'QuizDimFact', 'course_id', NULL);

UPDATE canvasdata_aux.CourseAssignments
LEFT JOIN QuizDimFact
   ON canvasdata_aux.CourseAssignments.course_id = QuizDimFact.course_id
  SET quiz_id                      = QuizDimFact.id,
      CourseAssignments.quiz_type  = QuizDimFact.quiz_type,
      quiz_name                    = QuizDimFact.name,
      CourseAssignments.due_at     = QuizDimFact.due_at,
      quiz_assignment_id           = QuizDimFact.assignment_id,
      quiz_time_limit              = QuizDimFact.time_limit,
      quiz_allowed_attempts        = QuizDimFact.allowed_attempts;

INSERT INTO canvasdata_aux.CourseAssignments (
                     quiz_id,         
                     quiz_type,
                     quiz_name,              
                     due_at,            
                     points_possible,  
                     quiz_time_limit,       
                     quiz_allowed_attempts, 
                     course_id
                     )
SELECT QuizDimFact.id AS id,
       QuizDimFact.quiz_type AS quiz_type,
       QuizDimFact.name AS name,
       QuizDimFact.due_at AS due_at,
       QuizDimFact.points_possible AS points_possible,
       QuizDimFact.time_limit AS time_limit,
       QuizDimFact.allowed_attempts AS allowed_attempts,
       QuizDimFact.course_id AS course_id
  FROM QuizDimFact LEFT JOIN canvasdata_aux.CourseAssignments
    ON QuizDimFact.course_id = canvasdata_aux.CourseAssignments.course_id
 WHERE canvasdata_aux.CourseAssignments.quiz_id is null;

USE canvasdata_aux;

# Fill in course name:
UPDATE CourseAssignments
  LEFT JOIN canvasdata_prd.course_dim
    ON id = course_id
 SET course_name = name,
     CourseAssignments.account_id  = course_dim.account_id
  WHERE workflow_state = 'available';

CREATE INDEX crs_nm_idx ON CourseAssignments(course_name(40));

# Fill in current_score
#     and final_score.

CREATE INDEX ass_grp_id_idx ON CourseAssignments(assignment_group_id);  

# Update current_score and final_score:

UPDATE CourseAssignments
 LEFT JOIN canvasdata_prd.assignment_group_score_fact
    USING(assignment_group_id, course_id)
 SET CourseAssignments.current_score = canvasdata_prd.assignment_group_score_fact.current_score,
     CourseAssignments.final_score = canvasdata_prd.assignment_group_score_fact.final_score;


CREATE INDEX assIdIndx on CourseAssignments(assignment_id);

# Fill in group_assignment_weight. 1 min:
UPDATE CourseAssignments
  LEFT JOIN canvasdata_prd.assignment_group_fact
    ON CourseAssignments.assignment_id = canvasdata_prd.assignment_group_fact.assignment_group_id
 SET group_assignment_weight = group_weight;

# Fill in the term_name:

UPDATE CourseAssignments
  LEFT JOIN Courses
    USING(course_id)
  SET CourseAssignments.term_name = Courses.term_name;
  
# Somehow, bad courses sneak into CourseAssignments above
# Those are non-available courses, such as ones that were deleted.
# Clean those out:

DELETE CourseAssignments
FROM CourseAssignments LEFT JOIN canvasdata_prd.course_dim
    ON course_id = id
 WHERE workflow_state != 'available';
