DROP TABLE IF EXISTS GradingProcess;
CREATE TABLE GradingProcess (
    account_id bigint,
    course_id bigint,
    course_name varchar(255),
    enrollment_term_id bigint,
    grader_id bigint,
    assignment_id bigint,
    submission_id bigint,
    user_id bigint,
    grade_letter varchar(255),
    grade_numeric double,
    grading_type varchar(256),
    grade_state varchar(36),
    group_assignment_final_score double,
    group_assignment_weight double,
    points_possible double
    ) engine=MyISAM;

# <end_creation>
    
call createIndexIfNotExists('account_id_idx', 'CourseAssignments', 'account_id, course_id', NULL);
# 1min 15sec:
call createIndexIfNotExists('account_id_idx', 'AssignmentSubmissions', 'account_id, course_id', NULL);

# 5 min 8sec
INSERT INTO GradingProcess
SELECT CourseAssignments.account_id,
       CourseAssignments.course_id,
       CourseAssignments.course_name,
       enrollment_term_id,
       grader_id,
       CourseAssignments.assignment_id,
       submission_id,
       user_id,
       grade_letter,
       grade_numeric,
       grading_type,
       grade_state,
       group_assignment_final_score,
       group_assignment_weight,
       CourseAssignments.points_possible
 FROM CourseAssignments LEFT JOIN AssignmentSubmissions
  USING(account_id, course_id)
 WHERE enrollment_term_id NOT IN (
       '35910000000000001',
       '35910000000000003',
       '35910000000000004',
       '35910000000000025'
       );

-- # Output CourseAssignments, AssignmentSubmissions, GradingProcess
-- # to csv. This happens on laptop:

-- # CourseAssignments:

-- #mysql -u canvasdata_prd -p -h canvasdata-prd-db1.ci6ilhrc8rxe.us-west-1.rds.amazonaws.com Andreas -N -B -e \
-- SELECT 'account_id','course_id','course_name','term_name','assignment_id','assignment_group_id',
--        'assignment_name','submission_types','points_possible','grading_type','assignment_state',
--        'workflow_state','due_date','group_assignment_name','group_assignment_weight',
--        'group_assignment_current_score','group_assignment_final_score'
-- UNION ALL
-- SELECT * INTO OUTFILE '/tmp/courseAssignments.tsv'
--     FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n'
--   FROM CourseAssignments;

-- # AssignmentSubmissions

-- #mysql -u canvasdata_prd -p -h canvasdata-prd-db1.ci6ilhrc8rxe.us-west-1.rds.amazonaws.com Andreas -N -B -e \
-- SELECT 'account_id','course_id','course_name','term_name','submission_id',
--        'assignment_id','assignment_name','assignment_description','quiz_submission_id',
--        'grader_id','user_id','enrollment_term_id','assignment_group_id',
--        'grade_letter','grade_numeric','points_possible','submitted_at',
--        'graded_at','grade_state','excused'
-- UNION ALL
-- SELECT * INTO outfile '/tmp/assignmentSubmissions.tsv'
--     FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n'
--   FROM AssignmentSubmissions;

-- # Grading process:

-- # mysql -u canvasdata_prd -p -h canvasdata-prd-db1.ci6ilhrc8rxe.us-west-1.rds.amazonaws.com Andreas -N -B -e \
-- SELECT 'reaccount_id','course_id','course_name','enrollment_term_id','grader_id',
--        'assignment_id','submission_id','user_id','grade_letter','grade_numeric',
--        'grading_type','grade_state','group_assignment_final_score',
--        'group_assignment_weight','points_possible'
-- UNION ALL
-- SELECT * INTO OUTFILE '/tmp/gradingProcess.tsv'
--     FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n'
-- FROM GradingProcess