UPDATE <canvas_aux>.CourseAssignments LEFT JOIN QuizDimFact ON
<canvas_aux>.CourseAssignments.course_id = QuizDimFact.course_id SET
quiz_id = QuizDimFact.id,;

INSERT INTO DiscussionMessages SELECT
     <canvas_db>.discussion_topic_dim.title AS disc_title, NULL AS
     disc_author_id,

LOAD DATA LOCAL INFILE '<data_dir>/explore_courses.csv' INTO TABLE
 ExploreCourses FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY '"'
 LINES TERMINATED BY '\n' IGNORE 1 LINES;
