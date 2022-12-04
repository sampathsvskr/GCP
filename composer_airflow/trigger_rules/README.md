# **Apache Airflow**

## **Trigger rules**

When you set dependencies between tasks, the default Airflow behavior is to run a task only when all upstream tasks have succeeded. You can use trigger rules to change this default behavior.

### References
**[Trigger Rules Aiflow doc](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#trigger-rules)** <br>
**[Artile by Marclamberti on Trigger Rules](https://marclamberti.com/blog/airflow-trigger-rules-all-you-need-to-know/)**

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

Lets see what will be the state of the current task with the respective trigger rule based on it's parent tasks state.
<br> 
For DAG py file which contains all these triggers, refer **[trigger_rule.py](https://github.com/sampathsvskr/GCP/blob/main/composer_airflow/trigger_rules/trigger_rule.py)**

Representation of task status in the table
- Success - S
- Failed - F
- Upstream failed - UF
- Skipped - SK
- None - N
- Will be executed - E

For ex: If parent task state is failed and current task has trigger rule "all_failed", then current gets executed, which is represented as success(S). Hope it will be suceess, cannot decide until the task completes, but refrence here is it will be exectuted because of trigger rule. <br> 

|Parent task state <br> Trigger rule	          | S  | F  | UF | SK | S,S | S,F | F,F | SK,SK | S,SK | SK,F | UF,UF | S,UF | F,UF | SK,UF | S,F,UF,SK | 
|-----------------	          | - | - | -- | -- | ---  | ---| ---| ------ | --- | --- | ----- | ---- | ---- | ---- | --------- |
|	all_done                                      | E  | E  | E  | E  | E  | E  | E  | E  | E  | E  | E  | E  | E  | E  | E  |
|	all_failed                                    | SK | E  | E  | SK | SK | SK | E  | SK | SK | SK | E  | SK | E  | SK | SK | 
|	all_skipped                                   | SK | SK | N  | E  | SK | SK | SK | E  | SK | SK | N  | SK | SK | N  | SK |
|	all_success                                   | E  | UF | UF | SK | E  | UF | UF | SK | SK | UF | UF | UF | UF | UF | UF |
|	always                                        | E  | E  | E  | E  | E  | E  | E  | E  | E  | E  | E  | E  | E  | E  | E |
|	dymmy                                         | E  | E  | E  | E  | E  | E  | E  | E  | E  | E  | E  | E  | E  | E  | E | 
|	none_failed_min_one_success                   | E  | UF | UF | SK | E  | UF | UF | SK | E  | UF | UF | UF | UF | UF | UF |
|	none_failed                                   | E  | UF | UF | E  | E  | UF | UF | E  | E  | UF | UF | UF | UF | UF | UF |
|	none_skipped                                  | E  | E  | E  | SK | E  | E  | E  | SK | SK | SK | E  | E  | E  | SK | SK |
|	one_failed                                    | SK | E  | E  | SK | SK | E  | E  | SK | SK | E  | E  | E  | E  | E  | E |
|   one_success                                   | E  | UF | UF | SK | E  | E  | UF | SK | E  | UF | UF | E  | UF | UF | E |
|   none_failed_or_skipped                           | | | | | | | | | | | |
