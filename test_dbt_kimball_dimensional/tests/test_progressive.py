import pytest
import subprocess
import json
from dataclasses import dataclass
import os

@dataclass
class Model:
    name:str
    success:bool
    error_message: str
    materialization:str
    rows:int 

@dataclass
class SchemaTestResult:
    name: str
    success: bool   

def assert_no_deltas(day:int):
    conn = create_engine(f"postgres://testkimball:testkimball@test_dbt_kimball_dimensional_postgres/testkimball")
    for table in ('DIM_USER','DIM_PRODUCT','FACT_SALE','FACT_RETURN',):
        sql = f""" WITH
                  expected AS (
                    SELECT * FROM {table}_DAY_{day}
                  EXCEPT
                    SELECT * FROM {table}
                  )
                  ,actual AS (
                    SELECT * FROM {table}
                  EXCEPT
                    SELECT * FROM {table}_DAY_{day}
                  )
                  SELECT * FROM expected
                    UNION
                  SELECT * FROM actual """             
        result = conn.execute(sql)
        assert len(result) == 0 ,f"{table} had deltas: {result}"
        
        

    
def run_day(day:int):
    ## runs all the models for a given day as datasets
    vars_day = f"""'{{"day_to_run": {day}}}'""" 
    run_args = ['dbt','run','--vars',vars_day]
    shell_string  = ' '.join(run_args)
    subprocess.run(shell_string, shell=True)
    with open('target/run_results.json','r') as f:
        results = json.loads(f.read())['results']
    models = list()
    for result in results:
        def get_row_count(result):
            potential_row_count = str(result['status']).split()[-1]
            if potential_row_count.isnumeric():
                return int(potential_row_count)
            elif potential_row_count == 'VIEW':
                return 1
            else:
                return 0
        models.append(Model(result['node']['name'],
              (result['error'] is None),
              result['error'],
              result['node']['config']['materialized'],
              get_row_count(result)
            ))
    return run_args, models

def run_schema_test():
    subprocess.run(['dbt',
                    'test'
                    ])
    with open('target/run_results.json','r') as f:
        results = json.loads(f.read())['results']
    test_results=list()
    for result in results:
        test_results.append(SchemaTestResult(result['node']['name'], (result['fail'] is None)))
    return test_results

def collect_a_day(day):
    conn=get_conn()
    run_args, models = run_a_day(day)
    for model in models:
        assert model.success, f"model {model.name} failed with error message: {model.error_message}."
    tests = run_schema_test()
    for test in tests:
        assert test.success, f"test {test.name} failed."

def test_day_one():
    collect_a_day(1)




