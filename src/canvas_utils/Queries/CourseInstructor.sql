DROP TABLE IF EXISTS CourseInstructor;
CREATE TABLE CourseInstructor (
    user_id bigint,
    course_id bigint,
    enrollment_term_id bigint,
    instructor_name varchar(255),
    course_name varchar(255),
    enrollment_term_name varchar(50)
    ) ENGINE=MyISAM;

INSERT INTO CourseInstructor
SELECT user_id,
       course_id,
       enrollment_term_id,
       instructor_name,
       NULL AS course_name,
       NULL AS enrollment_term_name
  FROM (SELECT <canvas_db>.enrollment_dim.user_id,
               <canvas_db>.course_dim.id AS course_id,
               <canvas_db>.course_dim.enrollment_term_id AS enrollment_term_id
          FROM <canvas_db>.course_dim LEFT JOIN <canvas_db>.enrollment_dim
               ON <canvas_db>.course_dim.id = <canvas_db>.enrollment_dim.course_id
            WHERE <canvas_db>.course_dim.workflow_state = 'available'
              AND <canvas_db>.enrollment_dim.workflow_state = 'active'
              AND <canvas_db>.course_dim.enrollment_term_id not in
                 ('35910000000000004','35910000000000003','35910000000000025','35910000000000001')
              AND <canvas_db>.enrollment_dim.type = 'TeacherEnrollment'
       ) AS CourseEnrl
 LEFT JOIN Instructors using(user_id);

CREATE INDEX crs_id_idx ON CourseInstructor(course_id);

# Fill in the course name 2.5min
UPDATE CourseInstructor
  LEFT JOIN <canvas_db>.course_dim
    ON CourseInstructor.course_id = <canvas_db>.course_dim.id
  SET CourseInstructor.course_name = <canvas_db>.course_dim.name;


# Fill in the enrollment term name 4min 16sec
UPDATE CourseInstructor
  LEFT JOIN <canvas_db>.enrollment_term_dim
    ON CourseInstructor.enrollment_term_id = <canvas_db>.enrollment_term_dim.id
  SET CourseInstructor.enrollment_term_name = enrollment_term_dim.name;
