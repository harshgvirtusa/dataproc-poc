from google.cloud import dataproc_v1 as dataproc
 
project_id = "your-project-id"
region = "us-central1"
bucket_name = "your-bucket-name"
 
def create_simple_template(template_name):
    client_options = {"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    client = dataproc.WorkflowTemplateServiceClient(client_options=client_options)
 
    template = dataproc.WorkflowTemplate()
    template.id = template_name
    template.placement.cluster_selector.cluster_labels['env'] = "testtoday"
 
    template.jobs = [
        {
            "pyspark_job": {
                "main_python_file_uri": f"gs://{bucket_name}/path/to/your_script.py",
                "args": [
                    "glue1234",
                    f"gs://{bucket_name}/path/to/input.csv",
                    f"gs://{bucket_name}/path/to/output/"
                ],
            },
            "step_id": f"{template_name}first",
        }
    ]
 
    request = dataproc.CreateWorkflowTemplateRequest(
        parent=f"projects/{project_id}/regions/{region}",
        template=template
    )
 
    # Make the request
    response = client.create_workflow_template(request=request)
    print(f"Template created with ID: {response.id}")
 
# Example usage
create_simple_template("simple-dataproc-workflow")
