from airflow.models import DagBag

def test_dag_loaded():
    dag_bag = DagBag()
    assert dag_bag.import_errors == {}

def test_fx_dag_exists():
    dag_bag = DagBag()
    dag = dag_bag.get_dag("exchange_rate_pipeline")
    assert dag is not None