from google.cloud import dataproc_v1 as dataproc
import json
 
project_id = "dnb-genai-gcp-sandbox-207616"
region = "us-central1"
bucket_name = "dataproc-bucket-sample"
 
def create_simple_template(template_name):
    client_options = {"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    client = dataproc.WorkflowTemplateServiceClient(client_options=client_options)
 
    template = dataproc.WorkflowTemplate()
    template.id = template_name
    template.placement.cluster_selector.cluster_labels['env'] = "test"
 
    template.jobs = [
        {
            "pyspark_job": {
                "main_python_file_uri": f"gs://{bucket_name}/pyspark_job.py", # path/to/your_script.py",
                "args": [
                    "pyspark_job",
                    f"gs://{bucket_name}/customers.csv", # path/to/input.csv",
                    f"gs://{bucket_name}/sample_output/" # path/to/output/"
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
template_name = "simple-dataproc-workflow"
create_simple_template(template_name)

workflow_template_client = dataproc.WorkflowTemplateServiceClient(
 client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
)
request = dataproc.InstantiateWorkflowTemplateRequest(
    name=f"projects/{project_id}/regions/{region}/workflowTemplates/{template_name}",
)

workflow_execution = workflow_template_client.instantiate_workflow_template(request)
print(workflow_execution.operation)
print(
    f"The workflow {template_name} submitted with name {workflow_execution.operation.name} Executed"
)
