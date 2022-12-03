# **Apache Airflow**

## **Trigger rules**

When you set dependencies between tasks, the default Airflow behavior is to run a task only when all upstream tasks have succeeded. You can use trigger rules to change this default behavior.
**[Trigger Rules Aiflow doc](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#trigger-rules)**

The following options are available:

- **all_success**: (default) The task runs only when all upstream tasks have succeeded.
- **all_failed**: The task runs only when all upstream tasks are in a failed or upstream_failed state.
- **all_done**: The task runs once all upstream tasks are done with their execution.
- **all_skipped**: The task runs only when all upstream tasks have been skipped.
- **one_failed**: The task runs as soon as at least one upstream task has failed.
- **one_success**: The task runs as soon as at least one upstream task has succeeded.
- **none_failed**: The task runs only when all upstream tasks have succeeded or been skipped.
- **none_failed_min_one_success**: The task runs only when all upstream tasks have not failed or upstream_failed, - and at least one upstream task has succeeded.
- **none_skipped**: The task runs only when no upstream task is in a skipped state.
- **always**: The task runs at any time.

<br><br>

Lets see, how tasks behave based on each trigger rule.

Representation of task status in the table
- Success - S
- Failed - F
- Upstream failed - UF
- Skipped - SK


|Parent task state <br> Trigger rule	          | S | F | UF | SK | S,S | S,F | F,F | SK,SK | S,SK | S,F | UF,UF | S,UF | F,UF | SK,UF | S,F,UF,SK | 
|-----------------	          | - | - | -- | -- | --- | --- | --- | ------ | --- | --- | ----- | ---- | ---- | ---- | --------- |
|	all_success    |  |  |  |  |  |  |  |  |  |  |  |  |  |  | |
|	all_failed    |  |  |  |  |  |  |  |  |  |  |  |  |  |  | | 
|	all_done    |  |  |  |  |  |  |  |  |  |  |  |  |  |  | |
|	all_skipped    |  |  |  |  |  |  |  |  |  |  |  |  |  |  | |
|	one_failed    |  |  |  |  |  |  |  |  |  |  |  |  |  |  | |
|	one_success    |  |  |  |  |  |  |  |  |  |  |  |  |  |  | | 
|	none_failed    |  |  |  |  |  |  |  |  |  |  |  |  |  |  | |
|	none_failed_min_one_success    |  |  |  |  |  |  |  |  |  |  |  |  |  |  | |
|	none_skipped    |  |  |  |  |  |  |  |  |  |  |  |  |  |  | |
|	always    |  |  |  |  |  |  |  |  |  |  |  |  |  |  | |
