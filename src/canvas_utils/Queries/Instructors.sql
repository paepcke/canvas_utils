DROP TABLE IF EXISTS Instructors;
CREATE TABLE Instructors (
    user_id bigint,
    instructor_name varchar(255)
    ) engine=MyISAM;

# <end_creation>


INSERT INTO Instructors
SELECT distinct user_id, user_dim.name AS instructor_name
  FROM (SELECT user_id, name
          FROM canvasdata_prd.enrollment_dim LEFT JOIN canvasdata_prd.role_dim
            ON role_id = role_dim.id
         WHERE role_dim.name = 'TeacherEnrollment'
       ) AS UserIdName
       LEFT JOIN canvasdata_prd.user_dim
         ON UserIdName.user_id = user_dim.id;
