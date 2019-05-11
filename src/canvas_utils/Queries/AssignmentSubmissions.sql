# Total: 1hr and 10min:

DROP TABLE IF EXISTS AssignmentSubmissions;
CREATE TABLE AssignmentSubmissions (
    account_id bigint,
    course_id bigint,
    course_name varchar(255),
    term_name varchar(60),
    submission_id bigint,
    assignment_id bigint,
    assignment_name varchar(255),
    assignment_description text,
    quiz_submission_id bigint,
    grader_id bigint,
    user_id bigint,
    enrollment_term_id bigint,
    assignment_group_id bigint,
    grade_letter varchar(255),
    grade_numeric double,
    points_possible double,
    submitted_at timestamp,
    graded_at timestamp,
    grade_state char(36),
    excused char(36),
    student_name varchar(255)
    ) engine=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

# <end_creation>


USE <canvas_db>;
CALL createIndexIfNotExists('assIdIndx',
                             'submission_fact',
                             'assignment_id',
                             NULL);

# 1min
CALL createIndexIfNotExists('enr_term_id_idx',
                             'submission_fact',
                             'enrollment_term_id',
                             NULL);
USE <canvas_aux>;


# 3min:
INSERT INTO AssignmentSubmissions
SELECT 
    submission_fact.account_id,
    submission_fact.course_id,
    NULL AS course_name,
    NULL AS term_name,
    submission_dim.id as submissionid,
    submission_dim.assignment_id,
    NULL AS assignment_title,
    NULL AS assignment_description,
    submission_dim.quiz_submission_id,
    submission_dim.grader_id,
    submission_dim.user_id,
    submission_fact.enrollment_term_id,
    submission_fact.assignment_group_id,
    submission_dim.published_grade AS grade_letter,
    submission_fact.published_score AS grade_numeric,
    NULL AS points_possible,
    submission_dim.submitted_at,
    submission_dim.graded_at,
    submission_dim.grade_state,
    submission_dim.excused,
    NULL AS student_name
 FROM <canvas_db>.submission_dim
  LEFT JOIN <canvas_db>.submission_fact
  ON <canvas_db>.submission_fact.submission_id = <canvas_db>.submission_dim.id
 AND <canvas_db>.submission_fact.assignment_id = <canvas_db>.submission_dim.assignment_id
 WHERE enrollment_term_id NOT IN (
       '35910000000000001',
       '35910000000000003',
       '35910000000000004',
       '35910000000000025'
       );

CALL createIndexIfNotExists('enr_trm_id_idx',
                            'AssignmentSubmissions',
                            'enrollment_term_id',
                            NULL);

CALL createIndexIfNotExists('crs_id_idx',
                            'AssignmentSubmissions',
                            'course_id',
                            NULL);
# Fill in the course name:
UPDATE AssignmentSubmissions
  LEFT JOIN <canvas_db>.course_dim
    ON AssignmentSubmissions.enrollment_term_id = <canvas_db>.course_dim.enrollment_term_id
   AND course_id = id
  SET AssignmentSubmissions.course_name = <canvas_db>.course_dim.name;

# ~1min:
CALL createIndexIfNotExists('ass_id',
                            'AssignmentSubmissions',
                            'assignment_id',
                             NULL);

USE <canvas_db>;
CALL createIndexIfNotExists('ass_id_idx',
                             'assignment_fact',
                             'assignment_id',
                             NULL);

CALL createIndexIfNotExists('crs_id_idx',
                             'assignment_fact',
                             'course_id',
                             NULL);
USE <canvas_aux>;

# Fill in the points_possible. 1.5min:
UPDATE AssignmentSubmissions
  LEFT JOIN <canvas_db>.assignment_fact
    USING(assignment_id, course_id)
  SET AssignmentSubmissions.points_possible = <canvas_db>.assignment_fact.points_possible;

# Add term name:

# 2.5 Minutes:
UPDATE AssignmentSubmissions
  LEFT JOIN Terms
    ON AssignmentSubmissions.enrollment_term_id = Terms.term_id
  SET AssignmentSubmissions.term_name = Terms.term_name;

# Add Assignment Title and description:
# 2.5 min:
UPDATE AssignmentSubmissions
  LEFT JOIN <canvas_db>.assignment_dim
    ON AssignmentSubmissions.assignment_id = <canvas_db>.assignment_dim.assignment_group_id
  SET AssignmentSubmissions.assignment_name = <canvas_db>.assignment_dim.title,
      AssignmentSubmissions.assignment_description = <canvas_db>.assignment_dim.description;

CREATE INDEX usr_id_idx ON AssignmentSubmissions(user_id);

# Add student name:
UPDATE AssignmentSubmissions
 LEFT JOIN Students using(user_id)
 SET AssignmentSubmissions.student_name = Students.student_name;
    
