### New Learned Skills:

- when pandas make enclousers with "" when the values have commas in them, it is to prevent the commas from being misinterpreted as delimiters when reading the CSV file. This is a common practice in CSV formatting to ensure that the data is correctly parsed, especially when values contain special characters like commas.
  **so when put it into snowflake we should make the file format csv with the option of field_optionally_enclosed_by = '"'**
  '''sql
  CREATE OR ALTER FILE FORMAT csv_format
  TYPE = CSV
  FIELD_DELIMITER = ","
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';
  '''

- JSON Serialization for XComs: Because mapped tasks pass data back and forth to a compilation task via XComs, we must ensure dates are passed as strings (since native Pandas timestamps cannot be serialized into JSON by default).
  **so we can use the isoformat() method to convert the timestamp to a string format that is JSON serializable.**

- To use .expand() with mapped tasks, the function must be decorated with @task and should return a list of dictionaries (or any JSON-serializable format) that can be expanded into individual tasks. Each dictionary in the list will be passed as keyword arguments to the mapped task and i should pass the list of dictionaries to the .expand() method when calling the mapped task. This allows Airflow to create a separate task instance for each dictionary in the list, effectively parallelizing the execution of the mapped task across all items in the list.
  **so we can return a list of dictionaries from the function that fetches the data and then use .expand() to create individual tasks for each dictionary in the list.**
  == how airflow handles all data comes from .expand() ==
  When you use .expand() in Airflow, it creates multiple task instances based on the list of dictionaries you provide. Each dictionary in the list is passed as keyword arguments to the mapped task, allowing for parallel execution. Airflow will automatically manage the execution of these tasks, ensuring that they run concurrently and efficiently. The results from each task can be collected and processed as needed, making it a powerful way to handle dynamic workloads in your DAGs.
  == so it works as concurrent execution and we can determine the number of concurrent working tasks to be 2 or 3 or more by setting the max_active_tasks parameter in the DAG definition. == but we should be careful when setting this parameter as it can lead to resource contention if set too high, especially if the tasks are resource-intensive. It's important to balance the need for parallelism with the available resources to ensure optimal performance of your DAGs.
