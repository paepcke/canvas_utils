USE canvasdata_aux;

DROP TABLE IF EXISTS ModuleItems;
CREATE TABLE ModuleItems (
    id bigint,
    title text COLLATE utf8mb4_unicode_ci,
    course_id bigint,
    module_id bigint,
    content_type varchar(50),
    requirement_type varchar(36),   # From module_progression_completion_requirement_dim via module_item_id
    completion_status varchar(36),  # From module_progression_completion_requirement_dim
    num_started int,                # See query below.
    num_completed int
    ) ENGINE=MyIsam;

USE canvasdata_prd;

# Populate ModuleItems:

INSERT INTO canvasdata_aux.ModuleItems
SELECT module_item_dim.id,
       title,
       course_id,
       module_id,
       content_type,
       requirement_type,
       completion_status,
       NULL AS num_started,
       NULL AS num_completed
  FROM module_item_dim LEFT JOIN module_progression_completion_requirement_dim
    ON module_item_dim.id = module_progression_completion_requirement_dim.module_item_id
  WHERE module_item_dim.workflow_state = 'active';
    
# Fill in num_started and num_completed:

CALL createIndexIfNotExists('mod_prog_fact_idx', 'module_progression_fact', 'module_progression_id', NULL);


UPDATE canvasdata_aux.ModuleItems
LEFT JOIN (
            SELECT module_progression_dim.module_id, count(*) AS num_module_progressions
              FROM module_progression_dim LEFT JOIN module_progression_fact
                ON module_progression_dim.id = module_progression_fact.module_progression_id
              WHERE workflow_state = 'started'
              GROUP BY module_progression_dim.module_id
          ) ProgressCount
     ON ProgressCount.module_id = canvasdata_aux.ModuleItems.module_id
 SET num_started = num_module_progressions;

UPDATE canvasdata_aux.ModuleItems
LEFT JOIN (
            SELECT module_progression_dim.module_id, count(*) AS num_module_progressions
              FROM module_progression_dim LEFT JOIN module_progression_fact
                ON module_progression_dim.id = module_progression_fact.module_progression_id
              WHERE workflow_state = 'completed'
              GROUP BY module_progression_dim.module_id
          ) ProgressCount
     ON ProgressCount.module_id = canvasdata_aux.ModuleItems.module_id
 SET num_completed = num_module_progressions;
