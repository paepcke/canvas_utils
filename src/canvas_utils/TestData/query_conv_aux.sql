UPDATE canvasdata_aux.CourseAssignments LEFT JOIN QuizDimFact ON
canvasdata_aux.CourseAssignments.course_id = QuizDimFact.course_id SET
quiz_id = QuizDimFact.id,;        
        self.tmp_file.write(sql_cmd)
