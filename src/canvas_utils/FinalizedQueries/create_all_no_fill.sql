# All the aux table creations and index creation.
# No filling with content.

DROP TABLE IF EXISTS Accounts;
CREATE TABLE Accounts (
    account_id bigint,
    account_name varchar(255)
    ) engine=MyISAM;


DROP TABLE IF EXISTS Accounts;
CREATE TABLE Accounts (
    account_id bigint,
    account_name varchar(255)
    ) engine=MyISAM;

CREATE INDEX usr_id_idx ON AllUsers(user_id);

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

CREATE INDEX enr_trm_id_idx ON AssignmentSubmissions(enrollment_term_id);
CREATE INDEX crs_id_idx     ON AssignmentSubmissions(course_id);
CREATE INDEX ass_id         ON AssignmentSubmissions(assignment_id);

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


CREATE INDEX crs_nm_idx ON CourseAssignments(course_name(40));
CREATE INDEX ass_grp_id_idx ON CourseAssignments(assignment_group_id);
CREATE INDEX crs_id_idx ON CourseAssignments(course_id);
CREATE INDEX ass_state_idx ON CourseAssignments(assignment_state(30));
CREATE INDEX assIdIndx ON CourseAssignments(assignment_id);
CREATE INDEX account_id_idx ON CourseAssignments(account_id, course_id);
CREATE INDEX account_id_idx ON AssignmentSubmissions(account_id, course_id);

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

CREATE INDEX crs_enrl_trm_idx ON CourseEnrollment(course_id, enrollment_term_id);
CREATE INDEX crs_enrl_trm_idx ON CourseInstructor(course_id, enrollment_term_id);
CREATE INDEX crs_enrl_trm_idx ON CourseInstructorTeams(course_id, enrollment_term_id);


DROP TABLE IF EXISTS CourseInstructor;
CREATE TABLE CourseInstructor (
    user_id bigint,
    course_id bigint,
    enrollment_term_id bigint,
    instructor_name varchar(255),
    course_name varchar(255),
    enrollment_term_name varchar(50)
    ) ENGINE=MyISAM;

CREATE INDEX crs_id_idx ON CourseInstructor(course_id);
CREATE INDEX enrl_trm_id_idx ON CourseInstructor(enrollment_term_id);

DROP TABLE IF EXISTS CourseInstructorTeams;


CREATE TABLE CourseInstructorTeams (
    course_id bigint,
    enrollment_term_id bigint,
    instructor_team text
    ) engine=MyISAM;


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

CREATE INDEX crs_nm_idx ON Courses(course_name(50));
CREATE INDEX crs_id_idx ON Courses(course_id);

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

CREATE INDEX disc_id_idx ON DiscussionMessages(disc_id);
CREATE INDEX disc_auth_id_idx ON DiscussionMessages(disc_author_id);

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

CREATE INDEX top_id_idx ON DiscussionTopics(disc_topic_id);
CREATE INDEX auth_id_idx ON DiscussionTopics(disc_topic_author_id);
CREATE INDEX crs_id_idx ON DiscussionTopics(disc_topic_course_id);

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

CREATE INDEX crs_nm_idx ON ExploreCourses(course_name(50));

DROP TABLE IF EXISTS Graders;
CREATE TABLE Graders (
    user_id bigint,
    grader_name varchar(255)
    ) engine=MyISAM;


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

DROP TABLE IF EXISTS Instructors;
CREATE TABLE Instructors (
    user_id bigint,
    instructor_name varchar(255)
    ) engine=MyISAM;

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

CREATE INDEX crs_id_indx ON RequirementsFill(stanford_course_id, quarter_name_canvas);

CREATE TABLE RequirementsFillUniq LIKE RequirementsFill;

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


CREATE INDEX usr_id_idx ON Instructors(user_id);

DROP TABLE IF EXISTS Students;
CREATE TABLE Students (
    user_id bigint,
    student_name varchar(255)
    ) engine=MyISAM;

DROP TABLE IF EXISTS TeachingAssistants;
CREATE TABLE TeachingAssistants (
    user_id bigint,
    ta_name varchar(255)
    ) engine=MyISAM;


DROP TABLE IF EXISTS Terms;
CREATE TABLE Terms (
    term_id bigint,
    term_name varchar(60),
    start_date timestamp
    );

CREATE INDEX trm_id_idx ON Terms(term_id);
