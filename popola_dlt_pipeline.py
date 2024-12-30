import dlt
from popola_dlt import eisa_source

def load_all_resources(start_date: str) -> None:
    try:
        # Create the pipeline
        pipeline = dlt.pipeline(
            pipeline_name="eisa",
            destination="postgres",  # Ensure destination is PostgreSQL
            dataset_name="eisa_data",
            export_schema_path="schemas/export",  # Add this line
            
        )

        # Create the data source
        source = eisa_source(start_date=start_date)

        # Get available resources
        available_resources = list(source._resources.keys())
        print(f"Available resources: {available_resources}")  # Log discovered resources

        # Run the pipeline and load data
        load_info = pipeline.run(source.with_resources(*available_resources))
        print(f"Pipeline Load Info: {load_info}")  # Print load info

    except Exception as e:
        print(f"Error running pipeline: {e}")

if __name__ == "__main__":
    load_all_resources(start_date="2000-01-01")
