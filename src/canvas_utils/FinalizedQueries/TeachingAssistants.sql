DROP TABLE IF EXISTS TeachingAssistants;
CREATE TABLE TeachingAssistants (
    user_id bigint,
    ta_name varchar(255)
    ) engine=MyISAM;

# <end_creation>


INSERT INTO TeachingAssistants
SELECT distinct user_id, user_dim.name AS ta_name
  FROM (SELECT user_id, name
          FROM canvasdata_prd.enrollment_dim LEFT JOIN canvasdata_prd.role_dim
            ON role_id = role_dim.id
         WHERE role_dim.name = 'TAEnrollment'
           AND enrollment_dim.workflow_state='active'         
       ) AS UserIdName
       LEFT JOIN canvasdata_prd.user_dim
         ON UserIdName.user_id = user_dim.id;

# CREATE INDEX uid_idx ON Students(user_id);
# CREATE INDEX uid_idx ON TeachingAssistants(user_id);
# CREATE INDEX uid_idx ON Instructors(user_id);
# CREATE INDEX uid_idx ON Graders(user_id);