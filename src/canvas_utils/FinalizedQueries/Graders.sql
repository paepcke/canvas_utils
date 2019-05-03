DROP TABLE IF EXISTS Graders;
CREATE TABLE Graders (
    user_id bigint,
    grader_name varchar(255)
    ) engine=MyISAM;

# <end_creation>

INSERT INTO Graders
SELECT distinct user_id, user_dim.name AS grader_name
  FROM (SELECT user_id, name
          FROM canvasdata_prd.enrollment_dim LEFT JOIN canvasdata_prd.role_dim
            ON role_id = role_dim.id
         WHERE role_dim.name = 'Grader'
           AND enrollment_dim.workflow_state='active'         
       ) AS UserIdName
       LEFT JOIN canvasdata_prd.user_dim
         ON UserIdName.user_id = user_dim.id;