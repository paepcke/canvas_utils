DROP TABLE IF EXISTS Terms;
CREATE TABLE Terms (
    term_id bigint,
    term_name varchar(60),
    start_date timestamp
    );

# <end_creation>

INSERT INTO Terms
SELECT DISTINCT id, name, date_start
  FROM canvasdata_prd.enrollment_term_dim;

CREATE INDEX trm_id_idx ON Terms(term_id);
