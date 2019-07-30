use canvasdata_aux;

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
    assignment_state varchar(255),
    workflow_state varchar(255),
    due_date timestamp,
    group_assignment_name varchar(255),
    group_assignment_weight double,
    group_assignment_current_score double,
    group_assignment_final_score double
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
    canvasdata_prd.assignment_dim.workflow_state as assignment_state,
    NULL AS workflow_state,
    canvasdata_prd.assignment_dim.due_at,
    canvasdata_prd.assignment_group_dim.name as group_assignment_name,
    NULL AS group_assignment_weight,        #  canvasdata_prd.assignment_group_fact.group_weight as group_assignment_weight,
    NULL AS group_assignment_current_score, # canvasdata_prd.assignment_group_score_fact.current_score as group_assignment_current_score,
    NULL AS group_assignment_final_score    # canvasdata_prd.assignment_group_score_fact.final_score as group_assignment_final_score
FROM canvasdata_prd.assignment_dim
    LEFT JOIN canvasdata_prd.assignment_group_dim
       ON canvasdata_prd.assignment_dim.assignment_group_id = canvasdata_prd.assignment_group_dim.id
      AND canvasdata_prd.assignment_dim.course_id           = canvasdata_prd.assignment_group_dim.course_id;

# 
CREATE INDEX crs_id_idx on CourseAssignments(course_id);

# Fill in course name:
UPDATE CourseAssignments
  LEFT JOIN canvasdata_prd.course_dim
    ON id = course_id
 SET course_name = name,
     CourseAssignments.account_id  = course_dim.account_id;


CREATE INDEX crs_nm_idx ON CourseAssignments(course_name(40));

USE canvasdata_prd;
CALL createIndexIfNotExists('nm_idx',
                             'course_dim',
                             'name',
                             NULL);

USE canvasdata_aux;

# Delete the entries that are sandbox tests:
DELETE CourseAssignments
  FROM CourseAssignments LEFT JOIN canvasdata_prd.course_dim
    ON CourseAssignments.course_name = canvasdata_prd.course_dim.name
 WHERE 
   enrollment_term_id IN (
       '35910000000000001',
       '35910000000000003',
       '35910000000000004',
       '35910000000000025'
       );

# Fill in group_assignment_current_score
#     and group_assignment_final_score.

CREATE INDEX ass_grp_id_idx ON CourseAssignments(assignment_group_id);
CREATE INDEX ass_state_idx ON CourseAssignments(assignment_state(30));

# Fill in workflow_state. 2min:
UPDATE CourseAssignments
  LEFT JOIN canvasdata_prd.assignment_group_dim
    ON CourseAssignments.assignment_id = canvasdata_prd.assignment_group_dim.id
  SET CourseAssignments.workflow_state = canvasdata_prd.assignment_group_dim.workflow_state;

# Update assignment_current_score and assignment_final_score:

UPDATE CourseAssignments
 LEFT JOIN canvasdata_prd.assignment_group_score_fact
    USING(assignment_group_id, course_id)
 SET CourseAssignments.group_assignment_current_score = canvasdata_prd.assignment_group_score_fact.current_score,
     CourseAssignments.group_assignment_final_score = canvasdata_prd.assignment_group_score_fact.final_score,
WHERE CourseAssignments.assignment_state IN('published','unpublished')
 AND CourseAssignments.workflow_state='available';

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
