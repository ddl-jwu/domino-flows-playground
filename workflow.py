from utils.flyte import DominoTask, Input, Output
from flytekitplugins.domino.helpers import run_domino_job_task, DominoJobTask, DominoJobConfig
from flytekit import workflow, dynamic
from flytekit.types.file import FlyteFile
from flytekit.types.directory import FlyteDirectory
from typing import TypeVar, Optional, List, Dict
import pandas as pd
from datetime import datetime, timedelta
from enum import Enum

class Weekday(Enum):
    MON = "Monday"
    TUE = "Tuesday"
    WED = "Wednesday"
    THU = "Thursday"
    FRI = "Friday"

@workflow
def training_workflow(data_path: str) -> FlyteFile: 
    """
    Sample data preparation and training workflow

    This workflow accepts a path to a CSV for some initial input and simulates
    the processing of the data and usage of the processed data in a training job.

    To run this workflowp, execute the following line in the terminal

    pyflyte run --remote workflow.py training_workflow --data_path /mnt/code/artifacts/data.csv

    :param data_path: Path of the CSV file data
    :return: The training results as a model
    """

    data_prep_results = DominoTask(
        name="Prepare data",
        command="python /mnt/code/scripts/prep-data.py",
        environment="Data Prep Environment",
        hardware_tier="Small",
        inputs=[
            Input(name="data_path", type=str, value=data_path),
        ],
        outputs=[
            Output(name="processed_data", type=FlyteFile)
        ]
    )

    training_results = DominoTask(
        name="Train model",
        command="python /mnt/code/scripts/train-model.py",
        environment="Training Environment",
        hardware_tier="Medium",
        inputs=[
            Input(name="processed_data", type=FlyteFile, value=data_prep_results['processed_data']),
            Input(name="epochs", type=int, value=10),
            Input(name="batch_size", type=int, value=32)
        ],
        outputs=[
            Output(name="model", type=FlyteFile)
        ]
    )

    return training_results['model']

@workflow
def training_subworkflow(data_path: str) -> FlyteFile: 

    data_prep_results = DominoTask(
        name="Prepare data",
        command="python /mnt/code/scripts/prep-data.py",
        environment="Data Prep Environment",
        hardware_tier="Small",
        inputs=[
            Input(name="data_path", type=str, value=data_path),
        ],
        outputs=[
            Output(name="processed_data", type=FlyteFile)
        ]
    )

    training_results = DominoTask(
        name="Train model",
        command="python /mnt/code/scripts/train-model.py",
        environment="Training Environment",
        hardware_tier="Medium",
        inputs=[
            Input(name="processed_data", type=FlyteFile, value=data_prep_results['processed_data']),
            Input(name="epochs", type=int, value=10),
            Input(name="batch_size", type=int, value=32)
        ],
        outputs=[
            Output(name="model", type=FlyteFile)
        ]
    )

    return training_results['model']

# pyflyte run --remote workflow.py generate_types 
@workflow
def generate_types(): 

    sce_types = DominoTask(
        name="Generate SCE Types",
        command="python /mnt/code/scripts/generate-sce-types.py",
        environment="Domino Standard Environment Py3.9 R4.3",
        hardware_tier="Small",
        inputs=[
            Input(name="sdtm_data_path", type=str, value="/some/path/to/data")
        ],
        outputs=[
            Output(name="pdf", type=FlyteFile[TypeVar("pdf")]),
            Output(name="sas7bdat", type=FlyteFile[TypeVar("sas7bdat")])
        ]
    )

    ml_types = DominoTask(
        name="Generate ML Types",
        command="python /mnt/code/scripts/generate-ml-types.py",
        environment="Domino Standard Environment Py3.9 R4.3",
        hardware_tier="Small",
        inputs=[
            Input(name="batch_size", type=int, value=32),
            Input(name="learning_rate", type=float, value=0.001),
            Input(name="do_eval", type=bool, value=True),
            Input(name="list", type=List[int], value=[1,2,3,4,5]),
            Input(name="date", type=datetime, value=datetime.now()),
            Input(name="duration", type=timedelta, value=timedelta(days=5)),
            Input(name="bytes", type=bytes, value=bytes([104, 101, 108, 108, 111])),
            Input(name="enum", type=Weekday, value=Weekday.MON),
            Input(
                name="dict", 
                type=Dict[str,int], 
                value={
                    'param1': 1, 
                    "param2": 2,
                    "param3": 3
                })
        ],
        outputs=[
            Output(name="csv", type=FlyteFile[TypeVar("csv")]),
            Output(name="json", type=FlyteFile[TypeVar("json")]),
            Output(name="png", type=FlyteFile[TypeVar("png")]),
            Output(name="jpeg", type=FlyteFile[TypeVar("jpeg")]),
            Output(name="notebook", type=FlyteFile[TypeVar("ipynb")]),
            Output(name="mlflow_model", type=FlyteDirectory)
        ]
    )

    return 

# pyflyte run --remote workflow.py training_workflow --data_path /mnt/code/artifacts/data.csv
@workflow
def training_workflow_git(data_path: str) -> FlyteFile: 
    """
    Sample data preparation and training workflow

    This workflow accepts a path to a CSV for some initial input and simulates
    the processing of the data and usage of the processed data in a training job.

    To run this workflowp, execute the following line in the terminal

    pyflyte run --remote workflow.py training_workflow --data_path /mnt/code/artifacts/data.csv

    :param data_path: Path of the CSV file data
    :return: The training results as a model
    """

    data_prep_results = DominoTask(
        name="Prepare data ",
        command="python /mnt/code/scripts/prep-data.py",
        environment="Data Prep Environment",
        hardware_tier="Small",
        inputs=[
            Input(name="data_path", type=str, value=data_path)
        ],
        outputs=[
            Output(name="processed_data", type=FlyteFile)
        ]
    )

    training_results = DominoTask(
        name="Train model ",
        command="python /mnt/code/scripts/train-model.py",
        environment="Training Environment",
        hardware_tier="Medium",
        inputs=[
            Input(name="processed_data", type=FlyteFile, value=data_prep_results['processed_data']),
            Input(name="epochs", type=int, value=10),
            Input(name="batch_size", type=int, value=32)
        ],
        outputs=[
            Output(name="model", type=FlyteFile)
        ]
    )

    return training_results['model']

# pyflyte run --remote workflow.py training_workflow_nested --data_path /mnt/code/artifacts/data.csv
@workflow
def training_workflow_nested(data_path: str): 

    model = training_subworkflow(data_path=data_path)

    results = DominoTask(
        name="Final task",
        command="sleep 100",
        environment="Training Environment",
        hardware_tier="Medium",
        inputs=[
            Input(name="model", type=FlyteFile, value=model)
        ]
    )

    return 


# pyflyte run --remote workflow.py training_workflow_mlflow
@workflow
def training_workflow_mlflow():
    """
    Sample mlflow training workflow

    To run this workflow, execute the following line in the terminal

    pyflyte run --remote workflow.py training_workflow_mlflow
    """

    data_prep_results = DominoTask(
        name="Run experiment",
        command="python /mnt/code/scripts/experiment.py",
        environment="Training Environment",
        hardware_tier="Medium",
    )

    return

@workflow
def pdf_copy(): 

    pdf = DominoTask(
        name="Copy PDF",
        command="sas -stdio scripts/file-copy.sas",
        environment="SAS Analytics Pro",
        hardware_tier="Small",
        outputs=[
            Output(name="report", type=FlyteFile[TypeVar("pdf")]),
        ]
    )

    return 

@workflow
def data_science_workflow(data_path: str): 
    """

    pyflyte run --remote workflow.py data_science_workflow --data_path /mnt/code/artifacts/data.csv

    """

    load_s3_data = DominoTask(
        name="Load S3 data",
        command="mkdir /workflow/outputs/data",
        environment="Data Prep Environment",
        hardware_tier="Small",
        inputs=[
            Input(name="data_path", type=str, value=data_path),
        ],
        outputs=[
            Output(name="data", type=FlyteFile)
        ]
    )

    load_snowflake_data = DominoTask(
        name="Load Snowflake data",
        command="mkdir /workflow/outputs/data",
        environment="Data Prep Environment",
        hardware_tier="Small",
        inputs=[
            Input(name="data_path", type=str, value=data_path),
        ],
        outputs=[
            Output(name="data", type=FlyteFile)
        ]
    )

    processed_data = DominoTask(
        name="Process data",
        command="mkdir /workflow/outputs/train_data",
        environment="Data Prep Environment",
        hardware_tier="Small",
        inputs=[
            Input(name="s3_data", type=FlyteFile, value=load_s3_data['data']),
            Input(name="snowflake_data", type=FlyteFile, value=load_snowflake_data['data'])
        ],
        outputs=[
            Output(name="train_data", type=FlyteFile)
        ]
    )

    train_model = DominoTask(
        name="Train model",
        command="mkdir /workflow/outputs/model",
        environment="Data Prep Environment",
        hardware_tier="Small",
        inputs=[
            Input(name="data", type=FlyteFile, value=processed_data['train_data'])
        ],
        outputs=[
            Output(name="model", type=FlyteFile)
        ]
    )

    evaluate_model = DominoTask(
        name="Evaluate model",
        command="sleep 10",
        environment="Data Prep Environment",
        hardware_tier="Small",
        inputs=[
            Input(name="data", type=FlyteFile, value=processed_data['train_data']),
            Input(name="model", type=FlyteFile, value=train_model['model'])
        ]
    )

    return 


@dynamic
def training_workflow_dynamic(data_path: str, name: str) -> FlyteFile: 
    """
    Sample data preparation and training workflow

    This workflow accepts a path to a CSV for some initial input and simulates
    the processing of the data and usage of the processed data in a training job.

    To run this workflowp, execute the following line in the terminal

    pyflyte run --remote workflow.py training_workflow_dynamic --data_path /mnt/code/artifacts/data.csv

    :param data_path: Path of the CSV file data
    :return: The training results as a model
    """

    data_prep_results = DominoTask(
        name=name,
        command="python /mnt/code/scripts/prep-data.py",
        environment="Data Prep Environment",
        hardware_tier="Small",
        inputs=[
            Input(name="data_path", type=str, value=data_path)
        ],
        outputs=[
            Output(name="processed_data", type=FlyteFile)
        ]
    )

    training_results = DominoTask(
        name="Train model ",
        command="python /mnt/code/scripts/train-model.py",
        environment="Training Environment",
        hardware_tier="Medium",
        inputs=[
            Input(name="processed_data", type=FlyteFile, value=data_prep_results['processed_data']),
            Input(name="epochs", type=int, value=10),
            Input(name="batch_size", type=int, value=32)
        ],
        outputs=[
            Output(name="model", type=FlyteFile)
        ]
    )

    return training_results['model']


@workflow
def simple_math_workflow(a: int, b: int) -> float:

    # Create first task
    add_task = DominoJobTask(
        name='Add numbers',
        domino_job_config=DominoJobConfig(Command="python add.py"),
        inputs={'first_value': int, 'second_value': int},
        outputs={'sum': int},
        use_latest=True
    )
    sum = add_task(first_value=a, second_value=b)

    # Create second task
    sqrt_task = DominoJobTask(
        name='Square root',
        domino_job_config=DominoJobConfig(Command="python sqrt.py"),
        inputs={'value': int},
        outputs={'sqrt': float},
        use_latest=True
    )
    sqrt = sqrt_task(value=sum)

    return sqrt