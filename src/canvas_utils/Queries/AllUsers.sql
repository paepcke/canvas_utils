DROP TABLE IF EXISTS AllUsers;
CREATE TABLE AllUsers (
    seq_num int AUTO_INCREMENT PRIMARY KEY,
    user_id bigint,
    name varchar(255),
    type varchar(30),
    role varchar(30),
    workflow_state varchar(20)
    ) engine=MyISAM;

# <end_creation>

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
    FROM canvasdata_prd.enrollment_dim;

# Add role (TeacherEnrollment, StudentEnrollment, etc.):

CREATE INDEX usr_id_idx ON AllUsers(user_id);

# 50sec:
UPDATE AllUsers
  LEFT JOIN canvasdata_prd.user_dim
    ON user_id = id
  SET AllUsers.name = user_dim.name;
