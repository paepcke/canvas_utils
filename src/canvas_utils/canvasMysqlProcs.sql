
# Set the statement delimiter to something other than ';'
# so the procedure can use ';':
delimiter //

#--------------------------
# createIndexIfNotExists
#-----------

# Create index if it does not exist yet.
# Parameter the_prefix_len can be set to NULL if not needed
# NOTE: ensure the database in which the table resides
# is the current db. I.e. do USE <db> before calling.

DROP PROCEDURE IF EXISTS createIndexIfNotExists //
CREATE PROCEDURE createIndexIfNotExists (IN the_index_name varchar(255),
                           IN the_table_name varchar(255),
                     IN the_col_name   varchar(255),
                     IN the_prefix_len INT)
this_proc: BEGIN
      # Check whether table exists:
      IF ((SELECT COUNT(*) AS table_exists
           FROM information_schema.tables
           WHERE TABLE_SCHEMA = DATABASE()
             AND table_name = the_table_name)
          = 0)
      THEN
           SELECT concat("**** Table ", DATABASE(), ".", the_table_name, " does not exist.");
       LEAVE this_proc;
      END IF;

      IF ((SELECT COUNT(*) AS index_exists
           FROM information_schema.statistics
           WHERE TABLE_SCHEMA = DATABASE()
             AND table_name = the_table_name
         AND index_name = the_index_name)
          = 0)
      THEN
          # Different CREATE INDEX statement depending on whether
          # a prefix length is required:
          IF the_prefix_len IS NULL
          THEN
                SET @s = CONCAT('CREATE INDEX ' ,
                                the_index_name ,
                        ' ON ' ,
                        the_table_name,
                        '(', the_col_name, ')');
          ELSE
                SET @s = CONCAT('CREATE INDEX ' ,
                                the_index_name ,
                       ' ON ' ,
                        the_table_name,
                        '(', the_col_name, '(',the_prefix_len,'))');
         END IF;
         PREPARE stmt FROM @s;
         EXECUTE stmt;
	 DEALLOCATE PREPARE stmt;
      END IF;
END//

#--------------------------
# createFulltextIndexIfNotExists
#-----------

# Create a fulltext index if it does not exist yet.
# NOTE: ensure the database in which the table resides
# is the current db. I.e. do USE <db> before calling.

DROP PROCEDURE IF EXISTS createFulltextIndexIfNotExists //
CREATE PROCEDURE createFulltextIndexIfNotExists (IN the_index_name varchar(255),
                     IN the_table_name varchar(255),
                     IN the_col_name   varchar(255))
this_proc: BEGIN
      # Check whether table exists:
      IF ((SELECT COUNT(*) AS table_exists
           FROM information_schema.tables
           WHERE TABLE_SCHEMA = DATABASE()
             AND table_name = the_table_name)
          = 0)
      THEN
           SELECT concat("**** Table ", DATABASE(), ".", the_table_name, " does not exist.");
           LEAVE this_proc;
      END IF;

      IF ((SELECT COUNT(*) AS index_exists
           FROM information_schema.statistics
           WHERE TABLE_SCHEMA = DATABASE()
             AND table_name = the_table_name
         AND index_name = the_index_name)
          = 0)
      THEN
           SET @s = CONCAT('CREATE FULLTEXT INDEX ' ,
                           the_index_name ,
                           ' ON ' ,
                           the_table_name,
                           '(', the_col_name, ')'
                           );
         PREPARE stmt FROM @s;
         EXECUTE stmt;
	 DEALLOCATE PREPARE stmt;
      END IF;
END//


#--------------------------
# dropIndexIfExists
#-----------

DROP PROCEDURE IF EXISTS dropIndexIfExists //
CREATE PROCEDURE dropIndexIfExists (IN the_table_name varchar(255),
                            IN the_col_name varchar(255))
BEGIN
    DECLARE indx_name varchar(255);
    IF ((SELECT COUNT(*) AS index_exists
         FROM information_schema.statistics
         WHERE TABLE_SCHEMA = DATABASE()
           AND table_name = the_table_name
         AND column_name = the_col_name)
        > 0)
    THEN
        SELECT index_name INTO @indx_name
    FROM information_schema.statistics
    WHERE TABLE_SCHEMA = DATABASE()
        AND table_name = the_table_name
        AND column_name = the_col_name;
        SET @s = CONCAT('DROP INDEX `' ,
                        @indx_name ,
                  '` ON ' ,
                the_table_name
                );
       PREPARE stmt FROM @s;
       EXECUTE stmt;
    END IF;
END//

#--------------------------
# addPrimaryIfNotExists
#----------------------

# Add primary key if it does not exist yet.

DROP PROCEDURE IF EXISTS addPrimaryIfNotExists //
CREATE PROCEDURE addPrimaryIfNotExists (IN the_table_name varchar(255),
                    IN the_col_name   varchar(255))
BEGIN
      IF ((SELECT COUNT(*) AS index_exists
           FROM information_schema.statistics
           WHERE TABLE_SCHEMA = DATABASE()
             AND table_name = the_table_name
         AND index_name = 'PRIMARY')
         = 0)
      THEN
          # 'IGNORE' will refuse to add duplicates:
            SET @s = CONCAT('ALTER IGNORE TABLE ' ,
                    the_table_name,
              ' ADD PRIMARY KEY ( ',
                    the_col_name,
              ' )'
              );
          PREPARE stmt FROM @s;
       EXECUTE stmt;
      END IF;
END//

#--------------------------
# dropPrimaryIfExists
#-----------

# Given a table name, drop its primary index
# if it exists.

DROP PROCEDURE IF EXISTS dropPrimaryIfExists //
CREATE PROCEDURE dropPrimaryIfExists (IN the_table_name varchar(255))
BEGIN
    IF ((SELECT COUNT(*) AS index_exists
         FROM information_schema.statistics
         WHERE TABLE_SCHEMA = DATABASE()
           AND table_name = the_table_name
         AND index_name = 'PRIMARY')
        > 0)
    THEN
        SET @s = CONCAT('ALTER TABLE ' ,
                the_table_name,
            ' DROP PRIMARY KEY;'
                );
       PREPARE stmt FROM @s;
       EXECUTE stmt;
    END IF;
END//

#--------------------------
# indexExists
#-----------

# Given a table and column name, return 1 if
# an index exists on that column, else returns 0.

DROP FUNCTION IF EXISTS indexExists//
CREATE FUNCTION indexExists(the_table_name varchar(255),
                        the_col_name varchar(255))
RETURNS BOOL
READS SQL DATA 
BEGIN
    IF ((SELECT COUNT(*)
         FROM information_schema.statistics
         WHERE TABLE_SCHEMA = DATABASE()
           AND table_name = the_table_name
           AND column_name = the_col_name) > 0)
   THEN
       RETURN 1;
   ELSE
       RETURN 0;
   END IF;
END//

#--------------------------
# anyIndexExists
#---------------

# Given a table return 1 if any non-PRIMARY
# index exists on that table, else returns 0.

DROP FUNCTION IF EXISTS anyIndexExists//
CREATE FUNCTION anyIndexExists(the_table_name varchar(255))
RETURNS BOOL
READS SQL DATA 
BEGIN
    IF ((SELECT COUNT(*)
         FROM information_schema.statistics
         WHERE TABLE_SCHEMA = DATABASE()
           AND table_name = the_table_name
           AND Index_name != 'Primary') > 0)
   THEN
       RETURN 1;
   ELSE
       RETURN 0;
   END IF;
END//

#--------------------------
# functionExists
#----------------

# Given a fully qualified function name, return 1 if
# the function exists in the respective db, else returns 0.
# Example: SELECT functionExists('Edx.idInt2Anon');

DROP FUNCTION IF EXISTS functionExists//
CREATE FUNCTION functionExists(fully_qualified_funcname varchar(255))
RETURNS BOOL
READS SQL DATA 
BEGIN
    SELECT SUBSTRING_INDEX(fully_qualified_funcname,'.',1) INTO @the_db_name;
    SELECT SUBSTRING_INDEX(fully_qualified_funcname,'.',-1) INTO @the_func_name;
    IF ((SELECT COUNT(*)
         FROM information_schema.routines
         WHERE ROUTINE_TYPE = 'FUNCTION'
           AND ROUTINE_SCHEMA = @the_db_name
           AND ROUTINE_NAME = @the_func_name) > 0)
   THEN
       RETURN 1;
   ELSE
       RETURN 0;
   END IF;
END//

#--------------------------
# grantExecuteIfExists
#---------------------

# Given a fully qualified function name, check whether
# the function exists. If it does, grant EXECUTE on it for
# everyone. Example: CALL grantExecuteIfExists('Edx.idInt2Anon');

DROP PROCEDURE IF EXISTS grantExecuteIfExists//
CREATE PROCEDURE grantExecuteIfExists(IN fully_qual_func_name varchar(255))
BEGIN
   IF functionExists(fully_qual_func_name)
   THEN
      SELECT CONCAT('GRANT EXECUTE ON FUNCTION ', fully_qual_func_name, " TO '%'@'%'")
        INTO @stmt;
      PREPARE stmt FROM @stmt;
      EXECUTE stmt;
   END IF;
END//

#--------------------------
# wordcount
#----------

# Returns number of words in argument. Usage example
#   SELECT SUM(wordcount(myCol)) FROM (SELECT myCol FROM myTable) AS Contents;
# returns the number of words in an entire text column.
# Taken from http://stackoverflow.com/questions/748276/using-sql-to-determine-word-count-stats-of-a-text-field

DROP FUNCTION IF EXISTS wordcount;
CREATE FUNCTION wordcount(str TEXT)
       RETURNS INT
       DETERMINISTIC
       SQL SECURITY INVOKER
       NO SQL
  BEGIN
    DECLARE wordCnt, idx, maxIdx INT DEFAULT 0;
    DECLARE currChar, prevChar BOOL DEFAULT 0;
    SET maxIdx=char_length(str);
    WHILE idx < maxIdx DO
        SET currChar=SUBSTRING(str, idx, 1) RLIKE '[[:alnum:]]';
        IF NOT prevChar AND currChar THEN
            SET wordCnt=wordCnt+1;
        END IF;
        SET prevChar=currChar;
        SET idx=idx+1;
    END WHILE;
    RETURN wordCnt;
  END
//


#--------------------------
# multipleDbQuery
#----------------

# Loop over multiple MySQL dbs (a.k.a. schemas), and
# issue the same query on each one. Place result into
# temp table ResultSet, which is overwritten with each
# call.
#
# Caller provides regex to identify the databases,
# a table name to look for within each database. Only
# dbs with that table present are involved in the query.
# A result field list, which defines the columns in the
# ResultSet table, a field list for the query, and where
# and group-by clauses. Example:
#
#    CALL multipleDbQuery(
#       '%coursera%',      -- regex to id the DBs to loop over
#       'hash_mapping',    -- tbl within each db
#       'user_id INT',     -- for CREATE TABLE ResultSet user_id INT...
#       'user_id',         -- for SELECT user_id FROM...
#       '1',               -- where clause is just 'True'
#       null)              -- no group-by

DROP PROCEDURE IF EXISTS multipleDbQuery //
CREATE PROCEDURE `multipleDbQuery`(dbNameRegex varchar(255),
                                   tableName varchar(255),
                   resFieldList varchar(255),
                   fieldList varchar(255),
                   whereClause varchar(255),
                   groupBy varchar(255))
proc_start_lbl: BEGIN
    declare scName varchar(250);
    declare q varchar(2000);
    declare progr varchar(255);
    declare tblCreate varchar(255);

    DROP TABLE IF EXISTS ResultSet;
    SET @tblCreate := concat('CREATE TEMPORARY TABLE ResultSet (',resFieldList,');');

    PREPARE stmt FROM @tblCreate;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    DROP TABLE IF EXISTS MySchemaNames;
    create temporary table MySchemaNames (
        schemaName varchar(250)
    );

    insert into MySchemaNames
    SELECT distinct
        TABLE_SCHEMA as SchemaName
    FROM
        `information_schema`.`TABLES`,
    `information_schema`.`SCHEMATA`
    where
        `information_schema`.`SCHEMATA`.`SCHEMA_NAME` LIKE dbNameRegex
      AND
        TABLE_NAME = tableName;

label1:
    LOOP
        set scName = (select schemaName from MySchemaNames limit 1);

    set @progr = concat("SELECT 'Retrieving user_id for `", scName,"`' AS Db\G;");
        PREPARE stmt0 FROM @progr;
    EXECUTE stmt0;
    DEALLOCATE PREPARE stmt0;

    if groupBy is NULL
    then
        set @q = concat('INSERT INTO ResultSet ',
                        'SELECT ', fieldList, ' FROM `', scName, '`.', tableName, ' WHERE ', whereClause);
    else
        set @q = concat('INSERT INTO ResultSet ',
                        'SELECT ', fieldList ,' FROM `', scName, '`.', tableName,' WHERE ', whereClause, ' GROUP BY ', groupBy);
    end if;
        PREPARE stmt1 FROM @q;
        EXECUTE stmt1;
        DEALLOCATE PREPARE stmt1;

        delete from MySchemaNames where schemaName = scName;
        IF ((select count(*) from MySchemaNames) > 0) THEN
            ITERATE label1;
        END IF;
        LEAVE label1;

    END LOOP label1;

    -- SELECT * FROM ResultSet;

    DROP TABLE IF EXISTS MySchemaNames;
    -- DROP TABLE IF EXISTS ResultSet;
END//

#--------------------------
# dateInQuarter
#-------------

# Tests whether a given date lies within a given *academic*
# year and quarter. Year is either a four digit number, or
# the string '%'. In this latter case, the test succeeds
# if the given date is in the proper quarter of any year.
# Numeric year may be provided as int or string.
#
# Quarter must be one of 'fall', 'winter', 'spring', 'summer'.
# Case does not matter.
#
# Examples:
#    SELECT dateInQuarter('2014-1-02', 'winter', '2013'); -> 1
#    SELECT dateInQuarter('2013-12-02', 'winter', '2013'); -> 1
#    SELECT dateInQuarter('2013-11-30', 'fall', 2013); -> 1
#    SELECT dateInQuarter('2014-03-30', 'spring', '%'); -> 1
#    SELECT dateInQuarter('2020-03-30', 'spring', '%'); -> 1


DROP FUNCTION IF EXISTS dateInQuarter//
CREATE FUNCTION dateInQuarter(dateInQuestion DATETIME, quarter varchar(6), academic_year varchar(4))
RETURNS BOOLEAN DETERMINISTIC
BEGIN
    DECLARE acQuarterNumber INT DEFAULT QUARTER(DATE_ADD(dateInQuestion, INTERVAL 1 MONTH));
    # If passed in wildcard, double it so that
    # conditionals below will work:
    IF (academic_year = '%')
    THEN
        SET academic_year := '%%';
    END IF;
    IF (acQuarterNumber = 4) # academic Fall
    THEN
        RETURN ((YEAR(dateInQuestion) LIKE academic_year) AND (LOWER(quarter) = 'fall'));
    ELSEIF (acQuarterNumber = 3) # academic Summer
    THEN
        # Unless year is wildcard, compute calendar year:
        SET academic_year := IF(academic_year = '%%','%', academic_year + 1);
        RETURN ((YEAR(dateInQuestion) LIKE academic_year) AND (LOWER(quarter) = 'summer'));
    ELSEIF (acQuarterNumber = 2) # academic Spring
    THEN
        # Unless year is wildcard, compute calendar year:
        SET academic_year := IF(academic_year = '%%','%', academic_year + 1);
        RETURN ((YEAR(dateInQuestion) LIKE academic_year) AND (LOWER(quarter) = 'spring'));
    ELSE # winter quarter: academic quarter straddles year boundary
        IF (academic_year = '%%')
    THEN
        RETURN(LOWER(quarter) = 'winter');
    END IF;
        IF (MONTH(dateInQuestion) = 12)
    THEN
        RETURN (YEAR(dateInQuestion) = academic_year);
        ELSE
        RETURN (YEAR(dateInQuestion) = academic_year + 1);
        END IF;
    END IF;
END//

#--------------------------
# makeUpperQuarterDate
#-------------

# Given a quarter name and academic year, return the
# latest date that is still within that quarter. Legal
# quarter arguments are fall,winter,spring, and summer.

DROP FUNCTION IF EXISTS makeUpperQuarterDate //
CREATE FUNCTION makeUpperQuarterDate(quarter varchar(6), academic_year INT)
RETURNS date DETERMINISTIC
BEGIN
    IF (quarter = 'fall')
    THEN
        RETURN DATE(concat(academic_year,'-11-30'));
    ELSEIF (quarter = 'winter')
    THEN
    RETURN DATE(concat(academic_year+1,'-02-28'));
    ELSEIF (quarter = 'spring')
    THEN
    RETURN DATE(concat(academic_year+1,'-05-31'));
    ELSEIF (quarter = 'summer')
    THEN
    RETURN DATE(concat(academic_year+1,'-08-31'));
    ELSE
        RETURN NULL;
    END IF;
END//

#--------------------------
# makeLowQuarterDate
#-------------

# Given a quarter name and academic year, return the
# earliest date that is within that quarter. Legal
# quarter arguments are fall,winter,spring, and summer.

DROP FUNCTION IF EXISTS makeLowQuarterDate //
CREATE FUNCTION `makeLowQuarterDate`(quarter varchar(6), academic_year INT)
RETURNS date DETERMINISTIC
BEGIN
    IF (quarter = 'fall')
    THEN
        RETURN DATE(concat(academic_year,'-09-01'));
    ELSEIF (quarter = 'winter')
    THEN
    RETURN DATE(concat(academic_year,'-12-01'));
    ELSEIF (quarter = 'spring')
    THEN
    RETURN DATE(concat(academic_year+1,'-03-01'));
    ELSEIF (quarter = 'summer')
    THEN
    RETURN DATE(concat(academic_year+1,'-06-01'));
    ELSE
        RETURN NULL;
    END IF;
END//

#------------------------------
# DATABASE_NAME
#-------------

DROP FUNCTION IF EXISTS DATABASE_NAME;

CREATE FUNCTION DATABASE_NAME(table_name varchar(255))
       RETURNS varchar(255)
DETERMINISTIC

BEGIN
/*
    Given either a fully qualified, or bare table name, return
    the table's database location. If a bare table name is passed,
    the current database is returned.

    Examples:    foo.bar => foo
                 bar     => result of DATABASE()

    This function is the equivalent of the Unix dirname
*/

    IF POSITION('.' IN table_name) = 0
    THEN
        -- No period found, so this is just a table name;
        -- return current db:
        RETURN(DATABASE());
    ELSE
        -- Grab the string before the period:
        RETURN SUBSTRING_INDEX(table_name, '.', 1);
    END IF;
END// # DATABASE_NAME


#------------------------------
# TABLE_BASENAME
#-------------

DROP FUNCTION IF EXISTS TABLE_BASENAME;

CREATE FUNCTION TABLE_BASENAME(table_name varchar(255))
       RETURNS varchar(255)
DETERMINISTIC

BEGIN
/*
    Given either a fully qualified, or bare table name, return
    the bare table name without the database. If a bare table
    name is passed, it is returned unchanged.

    Examples:    foo.bar => bar
                 bar     => bar

This function is the equivalent of the Unix basename
*/
    IF POSITION('.' IN table_name) = 0
    THEN
        -- No period found, so this is just a table name;
        RETURN(table_name);
    ELSE
        -- Grab the string after the period:
        RETURN(SUBSTRING(table_name FROM POSITION('.' IN table_name)+1));
    END IF;
END// # TABLE_BASENAME

#--------------------------
# desca
#-----------

# Given a table name, list its column names in
# alphabetical order: a 'desc' with alpha order.

DROP PROCEDURE IF EXISTS desca//

CREATE PROCEDURE desca(IN the_table_name varchar(255))
BEGIN
   DECLARE table_basename varchar(255);
   DECLARE table_db varchar(255);

   SET table_basename = TABLE_BASENAME(the_table_name);
   SET table_db       = DATABASE_NAME(the_table_name);

   SELECT distinct
       c.column_name,
       IF (c.character_maximum_length is not null,
                 concat(c.data_type, '(', c.character_maximum_length, ')'),
                 c.data_type) AS data_type
     FROM INFORMATION_SCHEMA.COLUMNS c
    WHERE c.table_name    = table_basename
      AND c.table_schema  = table_db
    ORDER BY c.column_name;
END//



# Restore standard delimiter:
delimiter ;
