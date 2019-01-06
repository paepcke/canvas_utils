# Table whose columns are requirements
# (GER, WAY, THINK, etc.). Rows are courses
# with 1 or 0 depending on whether a courses
# fills the column requirement. This table
# will have near-duplicates, b/c the Canvas
# course_id has multiple values for course
# differences within that same quarter that
# I don't understand. It's not one course_id
# for each section. It seems to be more
# obscure. We deal with uniquification afterwards:

DROP TABLE IF EXISTS RequirementsFill;
CREATE TABLE RequirementsFill (
    course_name varchar(255),
    course_id bigint,
    stanford_course_id int,
    quarter_name_canvas varchar(30),
    EC_EthicReas tinyint,
    WAY_ER tinyint,
    WAY_FR tinyint,
    DB_EngrAppSci tinyint,
    WAY_SMA tinyint,
    DB_NatSci tinyint,
    DB_SocSci tinyint,
    WAY_ED tinyint,
    WAY_SI tinyint,
    DB_Hum tinyint,
    Language tinyint,
    WAY_A_II tinyint,
    WAY_CE tinyint,
    EC_GlobalCom tinyint,
    WAY_AQR tinyint,
    DB_Math tinyint,
    EC_Gender tinyint,
    EC_AmerCul tinyint,
    Writing_2 tinyint,
    Writing_1 tinyint,
    THINK tinyint,
    IHUM_1 tinyint,
    Writing_SLE tinyint
    ) engine=MyISAM;

# <end_creation>

INSERT INTO RequirementsFill
SELECT course_name,
       course_id,
       stanford_course_id,
       quarter_name_canvas,
       IF(find_in_set('GER:EC-EthicReas', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS  EC_EthicReas,
       IF(find_in_set('WAY-ER', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS            WAY_ER,
       IF(find_in_set('WAY-FR', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS            WAY_FR,
       IF(find_in_set('GER:DB-EngrAppSci', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS DB_EngrAppSci,
       IF(find_in_set('WAY-SMA', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS           WAY_SMA,
       IF(find_in_set('GER:DB-NatSci', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS     DB_NatSci,
       IF(find_in_set('GER:DB-SocSci', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS     DB_SocSci,
       IF(find_in_set('WAY-ED', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS            WAY_ED,
       IF(find_in_set('WAY-SI', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS            WAY_SI,
       IF(find_in_set('GER:DB-Hum', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS        DB_Hum,
       IF(find_in_set('Language', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS          Language,
       IF(find_in_set('WAY-A-II', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS          WAY_A_II,
       IF(find_in_set('WAY-CE', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS            WAY_CE,
       IF(find_in_set('GER:EC-GlobalCom', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS  EC_GlobalCom,
       IF(find_in_set('WAY-AQR', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS           WAY_AQR,
       IF(find_in_set('GER:DB-Math', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS       DB_Math,
       IF(find_in_set('GER:EC-Gender', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS     EC_Gender,
       IF(find_in_set('GER:EC-AmerCul', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS    EC_AmerCul,
       IF(find_in_set('Writing 2', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS         Writing_2,
       IF(find_in_set('Writing 1', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS         Writing_1,
       IF(find_in_set('THINK', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS             THINK,
       IF(find_in_set('GER:IHUM-1', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS        IHUM_1,
       If(find_in_set('Writing SLE', REPLACE(ger_fulfillment, ' ','')) > 0,1,0) AS       Writing_SLE
  FROM Courses;

CREATE INDEX crs_id_indx ON RequirementsFill(stanford_course_id, quarter_name_canvas);

CREATE TABLE RequirementsFillUniq LIKE RequirementsFill;

INSERT INTO RequirementsFillUniq
SELECT t1.*
FROM RequirementsFill AS t1
LEFT JOIN RequirementsFill AS t2
ON t1.stanford_course_id = t2.stanford_course_id
AND t1.quarter_name_canvas = t2.quarter_name_canvas
AND t1.course_id > t2.course_id
WHERE t2.stanford_course_id IS NULL;
