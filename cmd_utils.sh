gcloud config set project pod-fr-retail
gsutil notification create -t projects/pod-fr-retail/topics/cloud_storage_notifications -f json gs://pod-fr-retail-demo
gsutil notification create -t projects/pod-fr-retail/topics/cloud_storage_notifications -f json -e OBJECT_FINALIZE gs://pod-fr-retail-demo 
gsutil notification list gs://pod-fr-retail-demo
gsutil notification delete projects/_/buckets/pod-fr-retail-demo/notificationConfigs/7
gsutil cp gs://dataflow-samples/shakespeare/kinglear.txt gs://pod-fr-retail-demo/test/kinglear-9.txt
gsutil cp -R gs://dataflow-samples/shakespeare/*.txt gs://pod-fr-retail-demo/test/
gsutil -m cp -R gs://dataflow-samples/wikipedia_edits/wiki_data-000000000*.json gs://pod-fr-retail-demo/test/
python3 main.py --dataflow_service_options=enable_prime --experiments=enable_vertical_memory_autoscaling


python3 dataflow_bq_storageapi_v2.py --dataflow_service_options=enable_prime --prebuild_sdk_container_engine=cloud_build --docker_registry_push_url=europe-west1-docker.pkg.dev/pod-fr-retail/demok8/beam_bq_storage_api
python3 dataflow_bq_storageapi_v2.py --dataflow_service_options=enable_prime --experiments=enable_vertical_memory_autoscaling --sdk_container_image=europe-west1-docker.pkg.dev/pod-fr-retail/demok8/beam_bq_storage_api/beam_python_prebuilt_sdk:latest
python3 dataflow_bq_storageapi_v2.py --dataflow_service_options=enable_prime  --sdk_container_image=europe-west1-docker.pkg.dev/pod-fr-retail/demok8/beam_bq_storage_api/beam_python_prebuilt_sdk:latest
cd dataflow-pipeline-examples
git add .
git commit -m "update"
git push -u origin main