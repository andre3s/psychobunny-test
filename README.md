# psychobunny-test
Test Data Engineer at PsychoBunny

- **`libraries/data_extraction.py` **: Defines the `extract_data` function that fetches data from S3.
- **`libraries/data_transforming.py` **: Defines the `data_transform` function for data processing.
- **`libraries/data_loading.py` **: Defines the `load_data` function that writes data to the database.

## Key Features
- **Dynamic Task Creation**: Generates extract, transform, and load tasks for multiple file types in the workflow.
- **Flexible and Configurable**: Allows easy extension to handle additional file types or data processing logic.
- **Modular Design**: Encapsulates extraction, transformation, and loading logic within reusable Python modules.
- **Logging and Monitoring**: Leverages Airflow's logging and UI for workflow monitoring and troubleshooting.

## Requirements
Before deploying and running the DAG, ensure the following prerequisites are met:
1. **Dependencies**:
    - Airflow and its required operators/decorators are installed.
    - All necessary libraries (`data_extraction`, `data_transforming`, and `data_loading`) are available in the `libraries` folder.

2. **Configuration**:
    - Update the S3 bucket and prefix as needed in the `extract_data` function.
    - Ensure database connectivity and name the tables (`customers` and `transactions`) correctly in the `load_data` function.

## How to Use
1. Add the DAG file (`psychobunny_dag.py`) and the `libraries` folder to your Airflow installation directory under `DAGS_FOLDER`.
2. Start the Airflow webserver and scheduler.
3. Navigate to the Airflow UI to enable and trigger the `psychobunny_dag`.
4. Monitor the DAG's execution, task logs, and run history to ensure successful workflow completion.

## Possible Future Enhancements
- **Retry Logic**: Add retry configurations for the tasks to handle transient data source issues or network failures.
- **Error Handling**: Capture detailed error logs and define fallback mechanisms.
- **Additional File Types**: Extend the DAG to process more file types by expanding the `file_types` list and corresponding logic.

## Contact

For any queries or issues, please contact me at [andre3s@proton.me](mailto:andre3s@proton.me).

