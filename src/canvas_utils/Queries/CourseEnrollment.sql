DROP TABLE IF EXISTS CourseEnrollment;
CREATE TABLE CourseEnrollment (
    course_id bigint(20),
    account_id bigint(20),
    enrollmentid bigint(20),
    enrollment_term_id bigint(20),
    enrollment_term_name varchar(50),
    course_name varchar(256),
    course_code varchar(256),
    start_at timestamp,
    enrollment_type varchar(256),
    user_id bigint(20),
    instructor_team text
    ) engine=MyISAM;

# <end_creation>


INSERT INTO CourseEnrollment
SELECT CrseEnrl.*, NULL AS instructor_team
 FROM (SELECT <canvas_db>.course_dim.id AS course_id,
              <canvas_db>.course_dim.account_id,
              <canvas_db>.enrollment_dim.id AS enrollment_id,
              <canvas_db>.course_dim.enrollment_term_id,
              NULL AS enrollment_term_name,
              <canvas_db>.course_dim.name AS course_name,
              <canvas_db>.course_dim.code AS course_code,
              <canvas_db>.course_dim.start_at,
              <canvas_db>.enrollment_dim.type AS enrollment_type,
              <canvas_db>.enrollment_dim.user_id
         FROM <canvas_db>.course_dim LEFT JOIN <canvas_db>.enrollment_dim
           ON <canvas_db>.course_dim.id = <canvas_db>.enrollment_dim.course_id
        WHERE <canvas_db>.course_dim.workflow_state = 'available'
          AND <canvas_db>.enrollment_dim.workflow_state = 'active'
          AND <canvas_db>.course_dim.enrollment_term_id not in
           ('35910000000000004','35910000000000003','35910000000000025','35910000000000001')
      ) AS CrseEnrl;

CREATE INDEX crs_enrl_trm_idx ON CourseEnrollment(course_id, enrollment_term_id);
CREATE INDEX crs_enrl_trm_idx ON CourseInstructor(course_id, enrollment_term_id);
CREATE INDEX crs_enrl_trm_idx ON CourseInstructorTeams(course_id, enrollment_term_id);

# Fill in the enrollment_term_name, and the instructor team:

UPDATE CourseEnrollment
 LEFT JOIN CourseInstructor USING(course_id, enrollment_term_id)
  SET CourseEnrollment.enrollment_term_name = CourseInstructor.enrollment_term_name;

UPDATE CourseEnrollment
 LEFT JOIN CourseInstructorTeams USING(course_id,enrollment_term_id)
 SET CourseEnrollment.instructor_team = CourseInstructorTeams.instructor_team;
