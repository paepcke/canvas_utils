# Before running this, run in BASH:

 -- Given EC_courses.xml, create CSV:

 -- ~/EclipseWorkspacesNew/canvas_utils/src/canvas_utils/explore_courses_etl.py \
 --     /Users/paepcke/Project/Pathways/Data/Tableau/Embeddings/*.xml > ~/tmp/explore_courses.csv
 

# At least for MySQL 8.x we need to allow zero dates,
# like '0000-00-00 00:00:00', which is found in the Canvas db:

SET sql_mode="ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";

# The target DB where all new tables will go:
CREATE DATABASE IF NOT EXISTS CanvasTest;
USE CanvasTest;

# Creating Terms
# --------------

DROP TABLE IF EXISTS Terms;
CREATE TABLE Terms (
    term_id bigint,
    term_name varchar(60),
    start_date timestamp
    );
INSERT INTO Terms
SELECT DISTINCT id, name, date_start
  FROM <canvas_db>.enrollment_term_dim;

CREATE index trm_id_idx ON terms(term_id);


# Creating Table AllUsers
#-----------------------

DROP TABLE IF EXISTS AllUsers;
CREATE TABLE AllUsers (
    user_id bigint,
    name varchar(255),
    type varchar(30),
    role varchar(30),
    workflow_state varchar(20)
    ) engine=MyISAM;

INSERT INTO AllUsers
  SELECT user_id,
         NULL AS name,
         type,
         case 
            when type = 'TeacherEnrollment'  THEN 'instructor'
            when type = 'StudentEnrollment'  THEN 'student'
            when type = 'TaEnrollment'       THEN 'TA'
            when type = 'DesignerEnrollment' THEN 'designer'
            when type = 'StudentEnrollment'  THEN 'student'
            when type = 'ObserverEnrollment' THEN 'observer'
         END,
         workflow_state
    FROM <canvas_db>.enrollment_dim;

# Add role (TeacherEnrollment, StudentEnrollment, etc.):

CREATE INDEX usr_id_idx ON AllUsers(user_id);

# 50sec:
UPDATE AllUsers
  LEFT JOIN <canvas_db>.user_dim
    ON user_id = id
  SET AllUsers.name = user_dim.name;
    
# Creating Table ExploreCourses
# -----------------------------

DROP TABLE IF EXISTS ExploreCourses;
CREATE TABLE ExploreCourses (
    course_code varchar(15),      # AA110
    subject varchar(15),          # AA
    course_name varchar(255),     # I Fly Away
    units_min int,                # 1
    units_max int,                # 3
    acad_year varchar(25),        # 2018-2019
    course_id int,                # Peoplesoft course id
    acad_group varchar(25),       # ENGR
    department varchar(40),       # AEROASTRO
    acad_career varchar(10),      # UG
    ger_fulfillment varchar(255), # THINK,WAY-ER,Writing 1
    quarter_name varchar(40),     # 'Fall', 'Summer', 'not given this year'
    instructors varchar(255)      # 'Jane Doe', 'Jane Doe, Ken Franklin'
    ) engine=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

# Pick up the Explore Courses parse of the EC .xml file we did
# ahead of loading this file:

LOAD DATA LOCAL INFILE '/tmp/explore_courses.csv'
 INTO TABLE ExploreCourses
  FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n'
  IGNORE 1 LINES;

CREATE INDEX crs_nm_idx ON ExploreCourses(course_name(50));

# Creating Courses Table
# ----------------------

DROP TABLE IF EXISTS Courses;
CREATE TABLE Courses (
    account_id bigint(20),
    course_id bigint(20),
    enrollment_term_id bigint(20),
    term_name varchar(60),
    account_name varchar(255),          # German Language (GERLANG)
    course_name varchar(255),           # I Fly Away
    stanford_course_id int,             # 2455342
    code varchar(255),                  # W19-MUSIC-276C-0
    date_end datetime,                  # 2019-04-01 00:00:00
    quarter_name_canvas varchar(16),    # Fall 2015, Default Term, Migrated Content
    quarter_name_peoplesoft varchar(40),# Fall, Summer, not given this year
    acad_year varchar(25),              # 2018-2019
    course_code varchar(30),            # AA110
    subject varchar(15),                # AA
    units_max int,                      # 3
    ger_fulfillment varchar(255),       # THINK,WAY-ER,Writing 1
    acad_group varchar(25),             # ENGR
    department varchar(40),             # AEROASTRO
    acad_career varchar(10),            # UG
    instructors varchar(255),           # 'Jane Doe', 'Jane Doe, Ken Franklin'
    enrollment int
    ) engine=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    
INSERT INTO Courses
SELECT account_id,
       course_dim.id AS course_id,
       enrollment_term_id,
       NULL AS term_name,
       NULL AS account_name,
       course_dim.name AS course_name,
       NULL AS stanford_course_id,
       code,
       NULL AS date_end,
       NULL AS quarter_name_canvas,
       NULL AS quarter_name_peoplesoft,
       NULL AS acad_year,
       NULL AS course_code,
       NULL AS subject,
       NULL AS units_max,
       NULL AS ger_fulfillment,
       NULL AS acad_group,
       NULL AS department,
       NULL AS acad_career,
       NULL AS instructors,
       NULL AS enrollment
 FROM <canvas_db>.account_dim LEFT JOIN <canvas_db>.course_dim
   ON <canvas_db>.account_dim.id = <canvas_db>.course_dim.account_id;

# Fill quarter_name (e.g. Fall 2015, Summer 2016),
# and date_end:

UPDATE Courses
  LEFT JOIN <canvas_db>.enrollment_term_dim
   ON Courses.enrollment_term_id = id
  SET quarter_name_canvas = name,
      Courses.date_end    = enrollment_term_dim.date_end;

# Fill in account_name:

UPDATE Courses
  LEFT JOIN <canvas_db>.account_dim
   ON account_id = id
  SET account_name = name;

# All else from ExploreCourses.

UPDATE Courses LEFT JOIN ExploreCourses USING(course_name) SET
   Courses.acad_year = ExploreCourses.acad_year,
   Courses.stanford_course_id = ExploreCourses.course_id,
   Courses.course_code = ExploreCourses.course_code, Courses.subject =
   ExploreCourses.subject, Courses.units_max =
   ExploreCourses.units_max, Courses.acad_group =
   ExploreCourses.acad_group, Courses.department =
   ExploreCourses.department, Courses.acad_career =
   ExploreCourses.acad_career, Courses.instructors =
   ExploreCourses.instructors, Courses.quarter_name_peoplesoft =
   ExploreCourses.quarter_name, Courses.ger_fulfillment =
   ExploreCourses.ger_fulfillment;

# Add enrollment:

UPDATE Courses
  LEFT JOIN
       (SELECT course_id,
              COUNT(course_id) AS enrollment
         FROM Courses LEFT JOIN <canvas_db>.enrollment_dim
            USING(course_id)
          GROUP BY course_id
       ) AS EnrlRes
   USING(course_id)
 SET Courses.enrollment = EnrlRes.enrollment;

# Now delete courses in which the canvas quarter
# does not match the peoplesoft quarter. Example
# canvas quarter value: "Winter 2015". An
# example of peoplesoft quarter_name is just "Winter"
# or "Fall".
# 
# Those mismatches indicate badly matched Canvas
# course offerings with Peoplesoft courses.
# 
# The following lowers the number of courses in Courses by
# about 30% (e.g. 96,448 to 64,304)

DELETE FROM Courses
  WHERE quarter_name_canvas IS NOT NULL
    and quarter_name_peoplesoft IS NOT NULL
    and quarter_name_peoplesoft != substring_index(quarter_name_canvas, ' ',1);

CREATE INDEX crs_nm_idx ON Courses(course_name(50));

# Add term name:

UPDATE Courses
  LEFT JOIN Terms
    ON Courses.enrollment_term_id = Terms.term_id
  SET Courses.term_name = Terms.term_name;

# Creating Table DiscussionTopics
# -------------------------------

DROP TABLE IF EXISTS DiscussionTopics;
CREATE TABLE DiscussionTopics (
    disc_topic_id bigint,
    disc_topic_author_id bigint,
    disc_topic_author_name varchar(255),
    disc_topic_author_role varchar(40),
    disc_topic_title varchar(256),
    course_enrollment_term_id bigint,
    course_account_id bigint,
    disc_topic_msg_length int,
    disc_topic_posted_at timestamp,
    disc_topic_course_id bigint,
    disc_topic_course_name varchar(255),
    disc_topic_course_code varchar(100),     # Canvas code: Sp15-ENGR-129-01
    disc_topic_subj_catnbr varchar(20),      # AA110, CS106A, ...
    disc_topic_course_start_date TIMESTAMP,
    disc_topic_type varchar(20)
    ) ENGINE = MyISAM;

INSERT INTO DiscussionTopics
    SELECT
           <canvas_db>.discussion_topic_dim.id AS disc_topic_id,
           NULL AS disc_topic_author_id,
           NULL AS disc_topic_author_name,
           NULL AS disc_topic_author_role,
           <canvas_db>.discussion_topic_dim.title AS disc_topic_title,
           <canvas_db>.course_dim.enrollment_term_id AS course_enrollment_term_id,
           <canvas_db>.course_dim.account_id AS course_account_id,
           NULL AS disc_topic_msg_length,
           <canvas_db>.discussion_topic_dim.posted_at AS disc_topic_posted_at,
           <canvas_db>.discussion_topic_dim.course_id AS disc_topic_course_id,
           <canvas_db>.course_dim.name AS disc_topic_course_name,
           <canvas_db>.course_dim.code AS disc_topic_course_code,
           NULL AS disc_topic_subj_catnbr,
           <canvas_db>.course_dim.start_at AS disc_topic_course_start_date,
           <canvas_db>.discussion_topic_dim.type AS disc_topic_type
      FROM <canvas_db>.discussion_topic_dim LEFT JOIN <canvas_db>.course_dim
       ON <canvas_db>.discussion_topic_dim.course_id = <canvas_db>.course_dim.id
     WHERE <canvas_db>.discussion_topic_dim.workflow_state = 'active';

# Add author id and message length:

CREATE INDEX top_id_idx ON DiscussionTopics(disc_topic_id);

USE <canvas_db>;
CALL createIndexIfNotExists('top_id_top_fact_idx',
                             'discussion_topic_fact',
                             'discussion_topic_id',
                             NULL);
USE <canvas_aux>;

UPDATE DiscussionTopics
  LEFT JOIN <canvas_db>.discussion_topic_fact
    ON DiscussionTopics.disc_topic_id = <canvas_db>.discussion_topic_fact.discussion_topic_id
  SET disc_topic_author_id  = user_id,
      disc_topic_msg_length = message_length;

CREATE INDEX auth_id_idx ON DiscussionTopics(disc_topic_author_id);

# Add author name and role 1min 30sec:
UPDATE DiscussionTopics
  LEFT JOIN AllUsers
    ON disc_topic_author_id   = user_id
  SET disc_topic_author_role  = role,
      disc_topic_author_name  = name;

# Add the catalog course name: AA110, etc.

CREATE INDEX crs_id_idx ON DiscussionTopics(disc_topic_course_id);
CREATE INDEX crs_id_idx ON Courses(course_id);

UPDATE DiscussionTopics
  LEFT JOIN Courses
   ON DiscussionTopics.disc_topic_course_id = Courses.course_id
 SET DiscussionTopics.disc_topic_subj_catnbr = Courses.course_code;

# Creating Table DiscussionMessages
# ----------------------------------

DROP TABLE IF EXISTS DiscussionMessages;
CREATE TABLE DiscussionMessages (
    disc_title varchar(256),
    disc_author_id bigint,
    disc_author_role varchar(40),
    disc_posted_at timestamp,
    disc_course_id bigint,
    disc_course_name varchar(255),
    disc_course_code varchar(100),
    disc_course_start_date TIMESTAMP,
    disc_id bigint,
    disc_type varchar(20)
    ) ENGINE = MyISAM;

# SET @COURSE_NAME = 'F18-CEE-120A-01/220A-01';
# SET @COURSE_ID_1   = '35910000000088714';


INSERT INTO DiscussionMessages
     SELECT <canvas_db>.discussion_topic_dim.title AS disc_title,
            NULL AS disc_author_id,
            NULL AS disc_author_role,
            <canvas_db>.discussion_topic_dim.posted_at AS disc_posted_at,
            <canvas_db>.discussion_topic_dim.course_id AS disc_course_id,
            <canvas_db>.course_dim.name AS disc_course_name,
            <canvas_db>.course_dim.code AS disc_course_code,
            <canvas_db>.course_dim.start_at AS disc_course_start_date,
            <canvas_db>.discussion_topic_dim.id AS disc_id,
            <canvas_db>.discussion_topic_dim.type AS disc_type
       FROM <canvas_db>.discussion_topic_dim LEFT JOIN <canvas_db>.course_dim
        ON <canvas_db>.discussion_topic_dim.course_id = <canvas_db>.course_dim.id
      WHERE <canvas_db>.discussion_topic_dim.workflow_state = 'active';

CREATE INDEX disc_id_idx ON DiscussionMessages(disc_id);

# Fill in the user_id:
UPDATE DiscussionMessages
 LEFT JOIN <canvas_db>.discussion_entry_fact
  ON DiscussionMessages.disc_id = <canvas_db>.discussion_entry_fact.discussion_entry_id
  SET disc_author_id = <canvas_db>.discussion_entry_fact.user_id;


CREATE INDEX disc_auth_id_idx ON DiscussionMessages(disc_author_id);

USE <canvas_db>;
CALL createIndexIfNotExists('usr_id_idx',
                             'enrollment_dim',
                             'user_id',
                             NULL);
USE <canvas_aux>;

# Fill in the user role "TeacherEnrollment, ..." (1 min):

UPDATE DiscussionMessages
 LEFT JOIN <canvas_db>.enrollment_dim
  ON DiscussionMessages.disc_author_id = <canvas_db>.enrollment_dim.user_id
  SET disc_author_role = <canvas_db>.enrollment_dim.type;


# Creating CourseAssignments/AssignmentSubmissions Tables
# -------------------------------------------------------

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

INSERT INTO CourseAssignments
SELECT
    NULL AS account_id,                    # <canvas_db>.assignment_group_score_fact.account_id,
    <canvas_db>.assignment_dim.course_id,
    NULL AS course_name,
    NULL AS term_name,
    <canvas_db>.assignment_dim.id as assignment_id,
    <canvas_db>.assignment_dim.assignment_group_id,
    <canvas_db>.assignment_dim.title as assignment_name,
    <canvas_db>.assignment_dim.submission_types,
    <canvas_db>.assignment_dim.points_possible,
    <canvas_db>.assignment_dim.grading_type,
    <canvas_db>.assignment_dim.workflow_state as assignment_state,
    NULL AS workflow_state,
    <canvas_db>.assignment_dim.due_at,
    <canvas_db>.assignment_group_dim.name as group_assignment_name,
    NULL AS group_assignment_weight,        #  <canvas_db>.assignment_group_fact.group_weight as group_assignment_weight,
    NULL AS group_assignment_current_score, # <canvas_db>.assignment_group_score_fact.current_score as group_assignment_current_score,
    NULL AS group_assignment_final_score    # <canvas_db>.assignment_group_score_fact.final_score as group_assignment_final_score
FROM <canvas_db>.assignment_dim
    LEFT JOIN <canvas_db>.assignment_group_dim
       ON <canvas_db>.assignment_dim.assignment_group_id = <canvas_db>.assignment_group_dim.id
      AND <canvas_db>.assignment_dim.course_id           = <canvas_db>.assignment_group_dim.course_id;

# Fill in course name:
UPDATE CourseAssignments
  LEFT JOIN <canvas_db>.course_dim
    ON id = course_id
 SET course_name = name;


CREATE INDEX crs_nm_idx ON CourseAssignments(course_name(40));

USE <canvas_db>;

CALL createIndexIfNotExists('nm_idx',
                             'course_dim',
                             'name',
                             NULL);
USE <canvas_aux>;

# Delete the entries that are sandbox tests:
DELETE CourseAssignments
  FROM CourseAssignments LEFT JOIN <canvas_db>.course_dim
    ON CourseAssignments.course_name = <canvas_db>.course_dim.name
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
CREATE INDEX crs_id_idx ON CourseAssignments(course_id);
CREATE INDEX ass_state_idx ON CourseAssignments(assignment_state(30));

# Fill in workflow_state. 2min:
UPDATE CourseAssignments
  LEFT JOIN <canvas_db>.assignment_group_dim
    ON CourseAssignments.assignment_id = <canvas_db>.assignment_group_dim.id
  SET CourseAssignments.workflow_state = <canvas_db>.assignment_group_dim.workflow_state;

# Update assignment_current_score, account_id, and
# assignment_final_score:

UPDATE CourseAssignments
 LEFT JOIN <canvas_db>.assignment_group_score_fact
    USING(assignment_group_id, course_id)
 SET CourseAssignments.group_assignment_current_score = <canvas_db>.assignment_group_score_fact.current_score,
     CourseAssignments.group_assignment_final_score = <canvas_db>.assignment_group_score_fact.final_score,
     CourseAssignments.account_id = <canvas_db>.assignment_group_score_fact.account_id
WHERE CourseAssignments.assignment_state IN('published','unpublished')
 AND CourseAssignments.workflow_state='available';

CREATE INDEX assIdIndx on CourseAssignments(assignment_id);

# Fill in group_assignment_weight. 1 min:
UPDATE CourseAssignments
  LEFT JOIN <canvas_db>.assignment_group_fact
    ON CourseAssignments.assignment_id = <canvas_db>.assignment_group_fact.assignment_group_id
 SET group_assignment_weight = group_weight;

# Fill in the term_name:

CREATE INDEX crs_id_idx ON CourseAssignments(course_id);
CREATE INDEX crs_id_idx ON Courses(course_id);

UPDATE CourseAssignments
  LEFT JOIN Courses
    USING(course_id)
  SET CourseAssignments.term_name = Courses.term_name;

# Create Table AssignmentSubmissions
# ----------------------------------

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
    excused char(36)
    ) engine=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

USE <canvas_db>;
CALL createIndexIfNotExists('assIdIndx',
                             'submission_fact',
                             'assignment_id',
                             NULL);

# 1min:
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
    submission_dim.excused
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

CREATE INDEX enr_trm_id_idx ON AssignmentSubmissions(enrollment_term_id);
CREATE INDEX crs_id_idx ON AssignmentSubmissions(course_id);

# Fill in the course name:
UPDATE AssignmentSubmissions
  LEFT JOIN <canvas_db>.course_dim
    ON AssignmentSubmissions.enrollment_term_id = <canvas_db>.course_dim.enrollment_term_id
   AND course_id = id
  SET AssignmentSubmissions.course_name = <canvas_db>.course_dim.name;

# ~1min:
CREATE INDEX ass_id ON AssignmentSubmissions(assignment_id);
CREATE INDEX ass_id ON <canvas_db>.assignment_fact(assignment_id);
CREATE INDEX crs_id ON <canvas_db>.assignment_fact(course_id);

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

# Create Table GradingProcess
# ---------------------------

# Info on grading:
DROP TABLE IF EXISTS GradingProcess;
CREATE TABLE GradingProcess (
    account_id bigint,
    course_id bigint,
    course_name varchar(255),
    enrollment_term_id bigint,
    grader_id bigint,
    assignment_id bigint,
    submission_id bigint,
    user_id bigint,
    grade_letter varchar(255),
    grade_numeric double,
    grading_type varchar(256),
    grade_state varchar(36),
    group_assignment_final_score double,
    group_assignment_weight double,
    points_possible double
    ) engine=MyISAM;
    
CREATE INDEX account_id_idx ON CourseAssignments(account_id, course_id);
# 1min 15sec:
CREATE INDEX account_id_idx ON AssignmentSubmissions(account_id, course_id);

# 5 min 8sec
INSERT INTO GradingProcess
SELECT CourseAssignments.account_id,
       CourseAssignments.course_id,
       CourseAssignments.course_name,
       enrollment_term_id,
       grader_id,
       CourseAssignments.assignment_id,
       submission_id,
       user_id,
       grade_letter,
       grade_numeric,
       grading_type,
       grade_state,
       group_assignment_final_score,
       group_assignment_weight,
       CourseAssignments.points_possible
 FROM CourseAssignments LEFT JOIN AssignmentSubmissions
  USING(account_id, course_id)
 WHERE enrollment_term_id NOT IN (
       '35910000000000001',
       '35910000000000003',
       '35910000000000004',
       '35910000000000025'
       );

# Output CourseAssignments, AssignmentSubmissions, GradingProcess
# to csv. This happens on laptop:

# CourseAssignments:

#mysql -u <canvas_db> -p -h canvasdata-prd-db1.ci6ilhrc8rxe.us-west-1.rds.amazonaws.com Andreas -N -B -e \
SELECT 'account_id','course_id','course_name','assignment_id','assignment_group_id','assignment_name ',
       'submission_types ','points_possible double','grading_type ','assignment_state ',
       'workflow_state ','due_date timestamp','group_assignment_name ',
       'group_assignment_weight','group_assignment_current_score',
       'group_assignment_final_score'
UNION ALL
SELECT * INTO OUTFILE '/tmp/courseAssignments.tsv'
    FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n'
  FROM CourseAssignments;

# AssignmentSubmissions

#mysql -u <canvas_db> -p -h canvasdata-prd-db1.ci6ilhrc8rxe.us-west-1.rds.amazonaws.com Andreas -N -B -e \
SELECT 'account_id','course_id','course_name','submission_id','assignment_id','quiz_submission_id',
       'grader_id','user_id','enrollment_term_id','assignment_group_id',
       'grade_letter','grade_numeric','points_possible','submitted_at','graded_at','grade_state','excused'
UNION ALL
SELECT * INTo outfile '/tmp/assignmentSubmissions.tsv'
    FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n'
  FROM AssignmentSubmissions;

# Grading process:

# mysql -u <canvas_db> -p -h canvasdata-prd-db1.ci6ilhrc8rxe.us-west-1.rds.amazonaws.com Andreas -N -B -e \
SELECT 'account_id','course_id','course_name','enrollment_term_id',
       'grader_id','assignment_id','submission_id','user_id',
       'grade_letter','grade_numeric','grading_type','grade_state',
       'group_assignment_final_score','group_assignment_weight',
       'points_possible'
UNION ALL
SELECT * INTO OUTFILE '/tmp/gradingProcess.tsv'
    FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n'
FROM GradingProcess

# Creating Instructors Table
# --------------------------

DROP TABLE IF EXISTS Instructors;
CREATE TABLE Instructors (
    user_id bigint,
    instructor_name varchar(255)
    ) engine=MyISAM;

INSERT INTO Instructors
SELECT distinct user_id, user_dim.name AS instructor_name
  FROM (SELECT user_id, name
          FROM <canvas_db>.enrollment_dim LEFT JOIN <canvas_db>.role_dim
            ON role_id = role_dim.id
         WHERE role_dim.name = 'TeacherEnrollment'
       ) AS UserIdName
       LEFT JOIN <canvas_db>.user_dim
         ON UserIdName.user_id = user_dim.id;

# Creating Accounts Table
# -----------------------

DROP TABLE IF EXISTS Accounts;
CREATE TABLE Accounts (
    account_id bigint,
    account_name varchar(255)
    ) engine=MyISAM;
    
INSERT INTO Accounts
SELECT id, name
  FROM <canvas_db>.account_dim;

# Creating Student Table
# ----------------------

DROP TABLE IF EXISTS Students;
CREATE TABLE Students (
    user_id bigint,
    student_name varchar(255)
    ) engine=MyISAM;

INSERT INTO Students
SELECT distinct user_id, user_dim.name AS student_name
  FROM (SELECT user_id, name
          FROM <canvas_db>.enrollment_dim LEFT JOIN <canvas_db>.role_dim
            ON role_id = role_dim.id
         WHERE <canvas_db>.role_dim.name = 'StudentEnrollment'
           AND <canvas_db>.enrollment_dim.workflow_state='active'
       ) AS UserIdName
       LEFT JOIN <canvas_db>.user_dim
         ON UserIdName.user_id = user_dim.id;

# Creating Instructors Table
# --------------------------

DROP TABLE IF EXISTS Instructors;
CREATE TABLE Instructors (
    user_id bigint,
    instructor_name varchar(255)
    ) engine=MyISAM;

INSERT INTO Instructors
  SELECT user_id,
         name AS instructor_name
    FROM <canvas_db>.enrollment_dim
      LEFT JOIN <canvas_db>.user_dim
     ON enrollment_dim.user_id = user_dim.id
   WHERE enrollment_dim.type = 'TeacherEnrollment';
         
# Creating Grader Table
# ---------------------

DROP TABLE IF EXISTS Graders;
CREATE TABLE Graders (
    user_id bigint,
    grader_name varchar(255)
    ) engine=MyISAM;

INSERT INTO Graders
SELECT distinct user_id, user_dim.name AS grader_name
  FROM (SELECT user_id, name
          FROM <canvas_db>.enrollment_dim LEFT JOIN <canvas_db>.role_dim
            ON role_id = role_dim.id
         WHERE role_dim.name = 'Grader'
           AND enrollment_dim.workflow_state='active'         
       ) AS UserIdName
       LEFT JOIN <canvas_db>.user_dim
         ON UserIdName.user_id = user_dim.id;

# Creating TA Table
# -----------------

DROP TABLE IF EXISTS TeachingAssistants;
CREATE TABLE TeachingAssistants (
    user_id bigint,
    ta_name varchar(255)
    ) engine=MyISAM;

INSERT INTO TeachingAssistants
SELECT distinct user_id, user_dim.name AS ta_name
  FROM (SELECT user_id, name
          FROM <canvas_db>.enrollment_dim LEFT JOIN <canvas_db>.role_dim
            ON role_id = role_dim.id
         WHERE role_dim.name = 'TAEnrollment'
           AND enrollment_dim.workflow_state='active'         
       ) AS UserIdName
       LEFT JOIN <canvas_db>.user_dim
         ON UserIdName.user_id = user_dim.id;

# CREATE INDEX uid_idx ON Students(user_id);
# CREATE INDEX uid_idx ON TeachingAssistants(user_id);
# CREATE INDEX uid_idx ON Instructors(user_id);
# CREATE INDEX uid_idx ON Graders(user_id);

# Creating Table StudentUnits
# ---------------------------

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

# Creating Table Quizzes
# ----------------------

CREATE TABLE QuizDim (
  `id` bigint(20) DEFAULT NULL,
  `canvas_id` bigint(20) DEFAULT NULL,
  `root_account_id` bigint(20) DEFAULT NULL,
  `name` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `points_possible` double DEFAULT NULL,
  `description` text COLLATE utf8mb4_unicode_ci,
  `quiz_type` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `course_id` bigint(20) DEFAULT NULL,
  `assignment_id` bigint(20) DEFAULT NULL,
  `workflow_state` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `scoring_policy` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `anonymous_submissions` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `display_questions` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `answer_display_order` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `go_back_to_previous_question` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `could_be_locked` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `browser_lockdown` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `browser_lockdown_for_displaying_results` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `browser_lockdown_monitor` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ip_filter` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `show_results` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `show_correct_answers` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `show_correct_answers_at` timestamp NULL DEFAULT NULL,
  `hide_correct_answers_at` timestamp NULL DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `published_at` timestamp NULL DEFAULT NULL,
  `unlock_at` timestamp NULL DEFAULT NULL,
  `lock_at` timestamp NULL DEFAULT NULL,
  `due_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  KEY `id_idx` (`id`),
  KEY `course_id_idx` (`course_id`),
  KEY `assignment_id_idx` (`assignment_id`)
) ENGINE=MyISAM;

# Creating Table CourseInstructor
# -------------------------------

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

# Creating Table CourseInstructorTeams
# ------------------------------------

# Multiple instructors as comma-separated names:

DROP TABLE IF EXISTS CourseInstructorTeams;
CREATE TABLE CourseInstructorTeams (
    course_id bigint,
    enrollment_term_id bigint,
    instructor_team text
    ) engine=MyISAM;
  
INSERT INTO CourseInstructorTeams
SELECT course_id,
       enrollment_term_id,
       GROUP_CONCAT(DISTINCT instructor_name SEPARATOR ', ') AS instructor_team
  FROM CourseInstructor
  GROUP BY course_id, enrollment_term_id;

# Creating Table CourseEnrollment
# -------------------------------

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


# Creating the RequirementsFill Table
# ------------------------------------

# Table whose columns are requirements
# (GER, WAY, THINK, etc.). Rows are courses
# with 1 or 0 depending on whether a courses
# fills the column requirement. This table
# will have near-duplicates, b/c the Canvas
# course_id has multiple values for course
# differences within that same quarter that
# I don't understand. It's not one course_id
# for each section. It seems to be more
# obscure. We deal with uniquification afterwards:

DROP TABLE IF EXISTS RequirementsFill;
CREATE TABLE RequirementsFill (
    course_name varchar(255),
    course_id bigint,
    stanford_course_id int,
    quarter_name_canvas varchar(30),
    EC_EthicReas tinyint,
    WAY_ER tinyint,
    WAY_FR tinyint,
    DB_EngrAppSci tinyint,
    WAY_SMA tinyint,
    DB_NatSci tinyint,
    DB_SocSci tinyint,
    WAY_ED tinyint,
    WAY_SI tinyint,
    DB_Hum tinyint,
    Language tinyint,
    WAY_A_II tinyint,
    WAY_CE tinyint,
    EC_GlobalCom tinyint,
    WAY_AQR tinyint,
    DB_Math tinyint,
    EC_Gender tinyint,
    EC_AmerCul tinyint,
    Writing_2 tinyint,
    Writing_1 tinyint,
    THINK tinyint,
    IHUM_1 tinyint,
    Writing_SLE tinyint
    ) engine=MyISAM;

INSERT INTO RequirementsFill
SELECT course_name,
       course_id,
       stanford_course_id,
       quarter_name_canvas,
       IF(find_in_set('GER:EC-EthicReas', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS  EC_EthicReas,
       IF(find_in_set('WAY-ER', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS            WAY_ER,
       IF(find_in_set('WAY-FR', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS            WAY_FR,
       IF(find_in_set('GER:DB-EngrAppSci', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS DB_EngrAppSci,
       IF(find_in_set('WAY-SMA', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS           WAY_SMA,
       IF(find_in_set('GER:DB-NatSci', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS     DB_NatSci,
       IF(find_in_set('GER:DB-SocSci', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS     DB_SocSci,
       IF(find_in_set('WAY-ED', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS            WAY_ED,
       IF(find_in_set('WAY-SI', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS            WAY_SI,
       IF(find_in_set('GER:DB-Hum', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS        DB_Hum,
       IF(find_in_set('Language', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS          Language,
       IF(find_in_set('WAY-A-II', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS          WAY_A_II,
       IF(find_in_set('WAY-CE', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS            WAY_CE,
       IF(find_in_set('GER:EC-GlobalCom', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS  EC_GlobalCom,
       IF(find_in_set('WAY-AQR', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS           WAY_AQR,
       IF(find_in_set('GER:DB-Math', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS       DB_Math,
       IF(find_in_set('GER:EC-Gender', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS     EC_Gender,
       IF(find_in_set('GER:EC-AmerCul', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS    EC_AmerCul,
       IF(find_in_set('Writing 2', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS         Writing_2,
       IF(find_in_set('Writing 1', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS         Writing_1,
       IF(find_in_set('THINK', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS             THINK,
       IF(find_in_set('GER:IHUM-1', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS        IHUM_1,
       If(find_in_set('Writing SLE', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS       Writing_SLE
  FROM Courses;

CREATE INDEX crs_id_indx ON RequirementsFill(stanford_course_id, quarter_name_canvas);

CREATE TABLE RequirementsFillUniq LIKE RequirementsFill;

INSERT INTO RequirementsFillUniq
SELECT t1.*
FROM RequirementsFill AS t1
LEFT JOIN RequirementsFill AS t2
ON t1.stanford_course_id = t2.stanford_course_id
AND t1.quarter_name_canvas = t2.quarter_name_canvas
AND t1.course_id > t2.course_id
WHERE t2.stanford_course_id IS NULL;

# Create Table ExploreCoursesHistory
# ----------------------------------

# We need the ce_instructor_courses and ce_explore_courses
# to reconstruct the ExploreCourses history before the current
# academic year. That's all that's included in the EC_courses.xml.
# So here the instructions for the two tables.

        -- # Create and Ingest ce_instructor_courses Table
        -- # ---------------------------------------------

        -- DROP TABLE IF EXISTS ce_instructor_courses;
        -- CREATE TABLE ce_instructor_courses (
        --     termcore int,
        --     evalunitid varchar(40),
        --     totalenrollment int,
        --     instructor_name varchar(255), # From 'instructors' in original
        --     instructor_id varchar(255),   # We will null out
        --     academic_cluster varchar(1),  # Always null
        --     acad_year varchar(10),        # '20062007'
        --     grading_basis varchar(40),    # Letter (ABCD/NP)
        --     units_min int,
        --     units_max int,
        --     subject varchar(16),
        --     catalog_nbr varchar(10),
        --     section_name varchar(10),     # LEC or LAB, etc.
        --     ger_all varchar(255),         # GER:DB-EngrAppSci
        --     tag1 varchar(5),
        --     tag2 varchar(5),
        --     tag3 varchar(5),
        --     tag4 varchar(5),
        --     all_offering_quarters varchar(15), # "SPR,WIN,AUT" or "WIN", etc.
        --     subject_flag_stem varchar(1), # Always NULL
        --     xlistname varchar(40),        # NA 0NANA
        --     xlistnames_ec varchar(255),   # "AA 215A;CME 215A", "AA238", etc.
        --     xlist_subj_flag varchar(1),   # Always null
        --     xlist_dept_codes varchar(1),  # Always null
        --     xlist_schools varchar(50),    # "School of Engineering", "Humanities&Sciences", etc.
        --     xlist_subjects varchar(1),    # Always null
        --     xlist_clusters varchar(1),    # Always null
        --     xlist_course_ids varchar(15), # "202,704", "103,141"
        --     full_course_name varchar(120),# "NA 0NANA (STRUCTURES, Sec 01)"
        --     course_name varchar(100),     # "SPACECRAFT DESIGN LABORATORY"
        --     section varchar(1),           # Always null
        --     eval_type varchar(10)         # We will null out.
        --     ) engine=MyISAM;
            
        -- LOAD DATA LOCAL INFILE '/Users/paepcke/Project/Canvas/Data/ce_instructor_courses.csv'
        --   INTO TABLE ce_instructor_courses
        --   FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n'
        --   IGNORE 1 LINES;

        -- # Null out instructor_id and eval_type:
        -- UPDATE ce_instructor_courses
        --   SET instructor_id = NULL,
        --       eval_type     = NULL;

        -- # Change the fused acad-year '20062007' into '2006-2007':
        -- UPDATE ce_instructor_courses
        --     set acad_year =  concat(substring(acad_year FROM 1 FOR 4),
        --                             '-', substring(acad_year FROM 5));

        -- # Add course_code: the concat of subject and cat#:
        -- ALTER TABLE ce_instructor_courses
        --   ADD COLUMN course_code varchar(40);

        -- UPDATE ce_instructor_courses
        --   SET course_code = CONCAT(subject,catalog_nbr);

        -- # Reverse order of instructor "last, first" name to be
        -- # "first last"
        -- UPDATE ce_instructor_courses
        -- SET instructor_name = concat(SUBSTRING_INDEX(instructor_name, ', ', -1),
        --                              ' ',
        --                              substring_index(instructor_name, ',', 1));

        -- # Create and Ingest ce_explore_courses Table
        -- # ------------------------------------------

        -- DROP TABLE IF EXISTS ce_explore_courses;
        -- CREATE TABLE ce_explore_courses (
        --     school_name varchar(255),          # Office of Vice Provost...
        --     department varchar(255),           # "Oversees Studies in Berlin"
        --     acad_year varchar(20),             # 20052006
        --     course_name varchar(255),          # Intro to Speaking
        --     course_description varchar(1000),
        --     grading_basis varchar(30),         # "Letter (ABCD/NP)", "Satisfactory/No Credit"
        --     units_min int,
        --     units_max int,
        --     subject varchar(30),
        --     catalog_nbr varchar(15),
        --     course_id int,
        --     offer_num int,
        --     section_comps varchar(10),         # SEM, LEC, etc.
        --     ger_all varchar(255),              # GER:DB-SocSci
        --     section_class_id int,
        --     section_num int,
        --     section_componenet varchar(10),    # SEM, LEC, etc.
        --     section_term varchar(20),          # 2006-2007 Autumn
        --     section_term_name varchar(50),     # AUT
        --     schedule varchar(100),             # 
        --     tag1 varchar(1),                   # Always null?
        --     tag2 varchar(1),                   # Always null?
        --     tag3 varchar(1),                   # Always null?
        --     tag4 varchar(1),                   # Always null?
        --     course_term_names varchar(30)      # "AUT", "NOTTHIS", "AUT,SPR", "WIN,AUT,SPR"
        --     ) engine=MyISAM;
            
        -- LOAD DATA LOCAL INFILE '/Users/paepcke/Project/Canvas/Data/ce_explore_courses.csv'
        --   INTO TABLE ce_explore_courses
        --   FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n'
        --   IGNORE 1 LINES;

# Now coalesce these tables into what we need in the ExploreCourses table
