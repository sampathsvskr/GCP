# **Apache Airflow**

## **Task Groups**
A TaskGroup is a collection of closely related tasks on the same DAG that should be grouped together when the DAG is displayed graphically.
A TaskGroup is only a visual-grouping feature in the UI.<br>
Currently, a TaskGroup is a visual-grouping feature nothing more, nothing less. <br>
Therefore, this implies two things: TaskGroups are very lightweight, and easy to use but, you don’t have as much control over the tasks in the group.<br> 

### References 
**[Artile by Marclamberti on Task Group](https://marclamberti.com/blog/airflow-taskgroups-all-you-need-to-know/)**
<br>
**[Simple DAG file with taskgroups](https://github.com/sampathsvskr/GCP/blob/main/composer_airflow/task_groups_and_branching/task_grp_dag.py)**

Image of the task groups created when using the above mentioned sample DAG file.<br><br>
![alt text](https://github.com/sampathsvskr/GCP/blob/main/composer_airflow/images/task_group.png)


## **Branching**
A way to choose tasks, like if else condition.<br>
You want to execute a task based on a condition? <br>
You multiple tasks but only one should be executed if a criterion is true? <br>
Then we can go with **BranchPythonOperator**

```python
def branch_fn(val):    
    if val>10:
        return "branch_task1"
    else:
        return "branch_task2"

select_barnch1 = BranchPythonOperator(task_id="select_barnch1", python_callable=branch_fn, op_args = [12], dag=dag)
branch_task1 = EmptyOperator(task_id="branch_task1", dag=dag)
branch_task2 = EmptyOperator(task_id="branch_task2", dag=dag)
```
### References 
**[Artile by Marclamberti branchoperator](https://marclamberti.com/blog/airflow-branchpythonoperator/)**
**[Simple DAG file with branching](https://github.com/sampathsvskr/GCP/blob/main/composer_airflow/task_groups_and_branching/branching_dag.py)**

Image of the DAG created when using the above mentioned sample DAG file without using trigger rules.<br><br>
![alt text](https://github.com/sampathsvskr/GCP/blob/main/composer_airflow/images/branching2.png)


Image of the DAG created when using the above mentioned sample DAG file.<br><br>
![alt text](https://github.com/sampathsvskr/GCP/blob/main/composer_airflow/images/branching_task.png)

### Points to note:
 - Airflow uses the default trigger rule as "all_success" i.e current task will be excuted, if all the parent tasks are success
  - When we use branch operator, only one of the tasks get excuted and the other is skipped.
 - As per airflow default trigger rule, the task after this branches will be triggered only if all parents are success. But here one will be skipped.
 - So if you see the first image, all the tasks after the branching task are skipped.
 - To make the other tasks to trigger even if the parent task is skipped, need to update the trigger rule for that task with "one_sucess" or "none_failed_or_skipped" ... based on your conditions.

 For more about trigger rules, refer **[trigger rules doc](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/trigger_rules)**





