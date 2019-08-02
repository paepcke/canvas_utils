USE canvasdata_aux;

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
    course_id bigint,
    course_name varchar(255),
    course_code varchar(100),                # Canvas code: Sp15-ENGR-129-01
    catnbr varchar(20),      # AA110, CS106A, ...
    disc_topic_course_start_date TIMESTAMP,
    disc_topic_type varchar(20),
    disc_topic_num_contribs int
    ) ENGINE = MyISAM;

# <end_creation>


# 2min:
INSERT INTO DiscussionTopics
    SELECT
           canvasdata_prd.discussion_topic_dim.id AS disc_topic_id,
           NULL AS disc_topic_author_id,
           NULL AS disc_topic_author_name,
           NULL AS disc_topic_author_role,
           canvasdata_prd.discussion_topic_dim.title AS disc_topic_title,
           canvasdata_prd.course_dim.enrollment_term_id AS course_enrollment_term_id,
           canvasdata_prd.course_dim.account_id AS course_account_id,
           NULL AS disc_topic_msg_length,
           canvasdata_prd.discussion_topic_dim.posted_at AS disc_topic_posted_at,
           canvasdata_prd.discussion_topic_dim.course_id AS course_id,
           canvasdata_prd.course_dim.name AS course_name,
           canvasdata_prd.course_dim.code AS course_code,
           NULL AS catnbr,
           canvasdata_prd.course_dim.start_at AS disc_topic_course_start_date,
           canvasdata_prd.discussion_topic_dim.type AS disc_topic_type,
           NULL AS disc_topic_num_contribs
      FROM canvasdata_prd.discussion_topic_dim LEFT JOIN canvasdata_prd.course_dim
       ON canvasdata_prd.discussion_topic_dim.course_id = canvasdata_prd.course_dim.id
     WHERE canvasdata_prd.discussion_topic_dim.workflow_state = 'active';

# Add author id and message length:

CREATE INDEX top_id_idx ON DiscussionTopics(disc_topic_id);

USE canvasdata_prd;
CALL createIndexIfNotExists('top_id_top_fact_idx',
                             'discussion_topic_fact',
                             'discussion_topic_id',
                             NULL);
USE canvasdata_aux;

# 15sec;
UPDATE DiscussionTopics
  LEFT JOIN canvasdata_prd.discussion_topic_fact
    ON DiscussionTopics.disc_topic_id = canvasdata_prd.discussion_topic_fact.discussion_topic_id
  SET disc_topic_author_id  = user_id,
      disc_topic_msg_length = message_length;

CREATE INDEX auth_id_idx ON DiscussionTopics(disc_topic_author_id);

# Add author name and role 30min:
# Need to disable indexing to go from 30hrs
# to 30 min:

ALTER TABLE DiscussionTopics DISABLE KEYS;
UPDATE DiscussionTopics
  LEFT JOIN AllUsers
    ON disc_topic_author_id   = user_id
  SET disc_topic_author_role  = role,
      disc_topic_author_name  = name;
ALTER TABLE DiscussionTopics  ENABLE KEYS;      

# Add the catalog course name: AA110, etc.

CREATE INDEX crs_id_idx ON DiscussionTopics(course_id);

UPDATE DiscussionTopics
  LEFT JOIN Courses
   ON DiscussionTopics.course_id = Courses.course_id
 SET DiscussionTopics.catnbr = Courses.course_code;

USE canvasdata_prd;
CALL createIndexIfNotExists('disc_entry_id_idx',
                            'discussion_entry_fact',
                            'topic_id',
                             NULL);

USE canvasdata_aux;

# Add number of entries in each discussion topic (i.e. in each row):
UPDATE DiscussionTopics
LEFT JOIN (SELECT count(*) AS num_entries, topic_id
             FROM DiscussionTopics LEFT JOIN canvasdata_prd.discussion_entry_fact
               ON disc_topic_id = topic_id
              GROUP BY topic_id
          ) AS TopicCount
    ON disc_topic_id = topic_id
  SET disc_topic_num_contribs = TopicCount.num_entries;

# 'Discussion' topic types show as NULL. Make those
# be 'Discussion' for clarity:

UPDATE DiscussionTopics
 SET disc_topic_type = 'Discussion'
WHERE disc_topic_type IS NULL;
