DROP TABLE IF EXISTS Instructors;
CREATE TABLE Instructors (
    user_id bigint,
    instructor_name varchar(255)
    ) engine=MyISAM;

# <end_creation>


INSERT INTO Instructors
SELECT distinct user_id, user_dim.name AS instructor_name
  FROM (SELECT user_id, name
          FROM <canvas_db>.enrollment_dim LEFT JOIN <canvas_db>.role_dim
            ON role_id = role_dim.id
         WHERE role_dim.name = 'TeacherEnrollment'
       ) AS UserIdName
       LEFT JOIN <canvas_db>.user_dim
         ON UserIdName.user_id = user_dim.id;


-- DROP TABLE IF EXISTS Instructors;
-- CREATE TABLE Instructors (
--     user_id bigint,
--     instructor_name varchar(255)
--     ) engine=MyISAM;

-- INSERT INTO Instructors
--   SELECT user_id,
--          name AS instructor_name
--     FROM <canvas_db>.enrollment_dim
--       LEFT JOIN <canvas_db>.user_dim
--      ON enrollment_dim.user_id = user_dim.id
--    WHERE enrollment_dim.type = 'TeacherEnrollment';
