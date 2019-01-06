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

# <end_creation>

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

# Fill in the user role "TeacherEnrollment, ..." (1 min):

UPDATE DiscussionMessages
 LEFT JOIN <canvas_db>.enrollment_dim
  ON DiscussionMessages.disc_author_id = <canvas_db>.enrollment_dim.user_id
  SET disc_author_role = <canvas_db>.enrollment_dim.type;

