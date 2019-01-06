DROP TABLE IF EXISTS Terms;
CREATE TABLE Terms (
    term_id bigint,
    term_name varchar(60),
    start_date timestamp
    );

# <end_creation>

INSERT INTO Terms
SELECT DISTINCT id, name, date_start
  FROM <canvas_db>.enrollment_term_dim;

CREATE index trm_id_idx ON terms(term_id);
