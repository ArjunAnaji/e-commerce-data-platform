import pytest
from airflow.models import DagBag

# Point to the DAGs directory or use sys.path.append to import correctly
DAG_PATHS = [
    "dags/extract_source_data_and_load_into_landing_zone.py",
    "dags/transform_data_and_load_into_ods.py",
    "dags/build_and_load_sales_order_item_flat_table.py",
]


@pytest.mark.parametrize("dag_path", DAG_PATHS)
def test_dag_import(dag_path):
    """
    Ensure each DAG file loads successfully without errors.
    """
    dag_bag = DagBag(dag_folder=dag_path, include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"Errors in {dag_path}: {dag_bag.import_errors}"


def test_dag_structure():
    """
    Test that all DAGs contain the expected tasks.
    """
    dag_bag = DagBag()

    # test 1: extract_source_data_and_load_into_landing_zone
    dag = dag_bag.get_dag("extract_source_data_to_landing_zone")
    assert dag is not None
    assert set(dag.task_ids) == {
        "extract_and_load_source_data"
    }

    # test 2: transform_data_and_load_into_ods
    dag = dag_bag.get_dag("transform_landing_data_and_load_to_ods")
    assert dag is not None
    assert set(dag.task_ids) == {
        "wait_for_extract_dag",
        "transform_and_load"
    }

    # test 3: build_and_load_sales_order_item_flat_table
    dag = dag_bag.get_dag("load_data_mart")
    assert dag is not None
    assert set(dag.task_ids) == {
        "wait_for_transform_dag",
        "load_sales_order_item_flat_table",
    }
