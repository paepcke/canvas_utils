# Multiple instructors as comma-separated names:

DROP TABLE IF EXISTS CourseInstructorTeams;
CREATE TABLE CourseInstructorTeams (
    course_id bigint,
    enrollment_term_id bigint,
    instructor_team text
    ) engine=MyISAM;
  
# <end_creation>


INSERT INTO CourseInstructorTeams
SELECT course_id,
       enrollment_term_id,
       GROUP_CONCAT(DISTINCT instructor_name SEPARATOR ', ') AS instructor_team
  FROM CourseInstructor
  GROUP BY course_id, enrollment_term_id;
