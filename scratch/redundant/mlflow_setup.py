# pyrefly: ignore [missing-import]
import mlflow
import os

def setup_mlflow():
    # Set the tracking URI to a local directory within main_folder
    mlflow_dir = os.path.join(os.getcwd(), "mlflow_data")
    if not os.path.exists(mlflow_dir):
        os.makedirs(mlflow_dir)
    
    mlflow.set_tracking_uri(f"file:///{mlflow_dir}")
    
    # Create or set the experiment
    experiment_name = "NIFTY_Data_Engine_Internship"
    mlflow.set_experiment(experiment_name)
    
    print(f"MLflow Tracking URI: {mlflow.get_tracking_uri()}")
    print(f"Experiment Name: {experiment_name}")

if __name__ == "__main__":
    setup_mlflow()
