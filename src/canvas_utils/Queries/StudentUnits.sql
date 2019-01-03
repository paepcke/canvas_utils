DROP TABLE IF EXISTS StudentUnits;
CREATE TABLE StudentUnits (
    account_id bigint,
    account_name varchar(255),
    enrollment_term_id bigint,
    quarter_name_canvas varchar(16),
    quarter_name_peoplesoft varchar(40),
    term_start timestamp,
    course_id bigint,
    course_name varchar(255),
    instructors varchar(255),
    units_max int,
    enrollment int
    ) engine=MyISAM;

# Fill in account_id, account_name, course_id,
# course_name, and enrollment:

INSERT INTO StudentUnits
SELECT Accounts.account_id,
       Accounts.account_name,
       enrollment_term_id,
       quarter_name_canvas,
       quarter_name_peoplesoft,
       NULL AS term_start,
       course_id,
       course_name,
       instructors,
       units_max,
       enrollment
  FROM Accounts LEFT JOIN Courses
   USING(account_id);

CREATE INDEX usr_id_idx ON Instructors(user_id);

# # Fill in the Instructor id and name. 9min:

# UPDATE StudentUnits,
#   (SELECT DISTINCT
#           course_id,
#           user_id,
#           enrollment_term_id,
#           user_id AS instructor_id,
#           instructor_name
#     FROM AssignmentSubmissions LEFT JOIN Instructors
#     USING(user_id)
#     WHERE Instructors.user_id IS NOT NULL 
#   ) AS InstructorInfo
#   SET StudentUnits.instructor_id    = InstructorInfo.user_id,
#       StudentUnits.instructor_name  = InstructorInfo.instructor_name;
