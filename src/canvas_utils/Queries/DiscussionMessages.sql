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
     SELECT canvasdata_prd.discussion_topic_dim.title AS disc_title,
            NULL AS disc_author_id,
            NULL AS disc_author_role,
            canvasdata_prd.discussion_topic_dim.posted_at AS disc_posted_at,
            canvasdata_prd.discussion_topic_dim.course_id AS disc_course_id,
            canvasdata_prd.course_dim.name AS disc_course_name,
            canvasdata_prd.course_dim.code AS disc_course_code,
            canvasdata_prd.course_dim.start_at AS disc_course_start_date,
            canvasdata_prd.discussion_topic_dim.id AS disc_id,
            canvasdata_prd.discussion_topic_dim.type AS disc_type
       FROM canvasdata_prd.discussion_topic_dim LEFT JOIN canvasdata_prd.course_dim
        ON canvasdata_prd.discussion_topic_dim.course_id = canvasdata_prd.course_dim.id
      WHERE canvasdata_prd.discussion_topic_dim.workflow_state = 'active';

CREATE INDEX disc_id_idx ON DiscussionMessages(disc_id);
USE canvasdata_prd;
CALL createIndexIfNotExists('user_id_idx',
                            'discussion_entry_fact',
                            'user_id',
                            NULL
                            );
CALL createIndexIfNotExists('msg_id_idx',
                            'discussion_entry_fact',
                            'discussion_entry_id',
                            NULL
                            );

USE canvasdata_aux;                            


# Fill in the user_id:
UPDATE DiscussionMessages
 LEFT JOIN canvasdata_prd.discussion_entry_fact
  ON DiscussionMessages.disc_id = canvasdata_prd.discussion_entry_fact.discussion_entry_id
  SET disc_author_id = canvasdata_prd.discussion_entry_fact.user_id;


CREATE INDEX disc_auth_id_idx ON DiscussionMessages(disc_author_id);

# Fill in the user role "TeacherEnrollment, ..." (1 min):

UPDATE DiscussionMessages
 LEFT JOIN canvasdata_prd.enrollment_dim
  ON DiscussionMessages.disc_author_id = canvasdata_prd.enrollment_dim.user_id
  SET disc_author_role = canvasdata_prd.enrollment_dim.type;
