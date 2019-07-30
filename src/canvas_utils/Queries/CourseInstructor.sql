DROP TABLE IF EXISTS CourseInstructor;
CREATE TABLE CourseInstructor (
    user_id bigint,
    course_id bigint,
    enrollment_term_id bigint,
    instructor_name varchar(255),
    course_name varchar(255),
    enrollment_term_name varchar(50)
    ) ENGINE=MyISAM;

# <end_creation>

INSERT INTO CourseInstructor
SELECT user_id,
       course_id,
       enrollment_term_id,
       instructor_name,
       NULL AS course_name,
       NULL AS enrollment_term_name
  FROM (SELECT canvasdata_prd.enrollment_dim.user_id,
               canvasdata_prd.course_dim.id AS course_id,
               canvasdata_prd.course_dim.enrollment_term_id AS enrollment_term_id
          FROM canvasdata_prd.course_dim LEFT JOIN canvasdata_prd.enrollment_dim
               ON canvasdata_prd.course_dim.id = canvasdata_prd.enrollment_dim.course_id
            WHERE canvasdata_prd.course_dim.workflow_state = 'available'
              AND canvasdata_prd.enrollment_dim.workflow_state = 'active'
              AND canvasdata_prd.course_dim.enrollment_term_id not in
                 ('35910000000000004','35910000000000003','35910000000000025','35910000000000001')
              AND canvasdata_prd.enrollment_dim.type = 'TeacherEnrollment'
       ) AS CourseEnrl
 LEFT JOIN Instructors using(user_id);

CREATE INDEX crs_id_idx ON CourseInstructor(course_id);

# Fill in the course name 2.5min
UPDATE CourseInstructor
  LEFT JOIN canvasdata_prd.course_dim
    ON CourseInstructor.course_id = canvasdata_prd.course_dim.id
  SET CourseInstructor.course_name = canvasdata_prd.course_dim.name;

CREATE INDEX enrl_trm_id_idx ON CourseInstructor(enrollment_term_id);

# Fill in the enrollment term name 4min 16sec
UPDATE CourseInstructor
  LEFT JOIN canvasdata_prd.enrollment_term_dim
    ON CourseInstructor.enrollment_term_id = canvasdata_prd.enrollment_term_dim.id
  SET CourseInstructor.enrollment_term_name = enrollment_term_dim.name;
