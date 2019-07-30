DROP TABLE IF EXISTS Students;
CREATE TABLE Students (
    user_id bigint,
    student_name varchar(255)
    ) engine=MyISAM;

# <end_creation>


INSERT INTO Students
SELECT distinct user_id, user_dim.name AS student_name
  FROM (SELECT user_id, name
          FROM canvasdata_prd.enrollment_dim LEFT JOIN canvasdata_prd.role_dim
            ON role_id = role_dim.id
         WHERE canvasdata_prd.role_dim.name = 'StudentEnrollment'
           AND canvasdata_prd.enrollment_dim.workflow_state='active'
       ) AS UserIdName
       LEFT JOIN canvasdata_prd.user_dim
         ON UserIdName.user_id = user_dim.id;

CREATE INDEX usr_id_idx ON Students(user_id);
