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

Lets see what the state of the current task with the respective trigger rule would be based on it's parent tasks state.

Representation of task status in the table
- Success - S
- Failed - F
- Upstream failed - UF
- Skipped - SK
- None - N


|Parent task state <br> Trigger rule	          | S  | F  | UF | SK | S,S | S,F | F,F | SK,SK | S,SK | SK,F | UF,UF | S,UF | F,UF | SK,UF | S,F,UF,SK | 
|-----------------	          | - | - | -- | -- | ---  | ---| ---| ------ | --- | --- | ----- | ---- | ---- | ---- | --------- |
|	all_done                                      | S  | S  | S  | S  | S  | S  | S  | S  | S  | S  | S  | S  | S  | S  | S  |
|	all_failed                                    | SK | S  | S  | SK | SK | SK | S  | SK | SK | SK | S  | SK | S  | SK | SK | 
|	all_skipped                                   | SK | SK | N  | S  | SK | SK | SK | S  | SK | SK | N  | SK | SK | N  | SK |
|	all_success                                   | S  | UF | UF | SK | S  | UF | UF | SK | SK | UF | UF | UF | UF | UF | UF |
|	always                                        | S  | S  | S  | S  | S  | S  | S  | S  | S  | S  | S  | S  | S  | S  | S |
|	dymmy                                         | S  | S  | S  | S  | S  | S  | S  | S  | S  | S  | S  | S  | S  | S  | S | 
|	none_failed_min_one_success                   | S  | UF | UF | SK | S  | UF | UF | SK | S  | UF | UF | UF | UF | UF | UF |
|	none_failed                                   | S  | UF | UF | S  | S  | UF | UF | S  | S  | UF | UF | UF | UF | UF | UF |
|	none_skipped                                  | S  | S  | S  | SK | S  | S  | S  | SK | SK | SK | S  | S  | S  | SK | SK |
|	one_failed                                    | SK | S  | S  | SK | SK | S  | S  | SK | SK | S  | S  | S  | S  | S  | S |
|   one_success                                   | S  | UF | UF | SK | S  | S  | UF | SK | S  | UF | UF | S  | UF | UF | S |
