CREATE SCHEMA IF NOT EXISTS employee_performance_schema;

CREATE TABLE IF NOT EXISTS employee_performance_schema.employee_performance (
    employee_id INT,
    name STRING,
    current_skill INT,
    initial_skill INT,
    skill_improvement INT
);

-- Add your Snowpipe automation for loading the data from stage
CREATE OR REPLACE PIPE employee_performance_schema.employee_performance_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO employee_performance_schema.employee_performance
  FROM @your_stage_name
  FILE_FORMAT = (TYPE = 'CSV');
