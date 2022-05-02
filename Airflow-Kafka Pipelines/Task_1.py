from airflow import DAG
from airflow.operators.bash import BashOperator #importing the operator'
import datetime as dt 


# We set our default arguments in order to use them in every task.

default_args = {'owner ' : 'me' , 
                'start_date': dt.datetime(2022,4,12),
                'retries' : 1 ,
                'retry_delay' : dt.timedelta(minutes=5)
                }

dag = DAG("tasks",description ='tasks',
                  default_args=default_args,
                  schedule_interval = dt.timedelta(seconds=5))

# Task 1.1: Creates a string variable called “firstName” assigning to it the string that corresponds to your first name with all letters in lowercase.
firstname_import = BashOperator(task_id='firstName',
                                bash_command= ' firstName="ilias" ',
                                dag=dag)


# Task 1.2: Creates a string variable called “lastName” assigning to it the string that corresponds to you last name with all letters in lowercase.
lastname_import = BashOperator(task_id='lastName',
                               bash_command= ' lastName="dimos"  ',
                               dag=dag)


# Task 2.1: It gets the firstName as input and outputs the same string with the first letter capitalized.
firstName_cap = BashOperator(task_id='firstName_cap',
                             bash_command = ' firstName=$(echo ${firstName^}) ', # Outcome : Ilias
                             dag=dag)
# Task 2.2: It gets the lastName as input and outputs the same string with the first letter capitalized.
lastName_cap = BashOperator(task_id='lastName_cap',
                            bash_command = ' lastName=$(echo ${lastName^}) ', # Outcome : Dimos
                            dag=dag)
#Task 3: Displays on screen a string that concatenates the firstName and the lastName adding a space character in between.
Concatenating_the_Results = BashOperator(task_id='Concatenating_the_Results',
                                         bash_command = ' echo $firstName $lastName ',
                                         dag=dag)

# In this task we setted up the order of how the tasks will be excecuted.
#First, we import the first name and then the last name as string variables, 
# we Uppercase the first letter of the two variables, and finally, we concatenate the results.

firstname_import >> lastname_import >>  firstName_cap >> lastName_cap >> Concatenating_the_Results

#Pipeline Outcome : Ilias Dimos