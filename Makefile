.PHONY: upload_dags
upload_dags:
	aws s3 sync dags/ s3://dev-gametaco-airflow/dags/ --delete

.PHONY: upload_requirements
upload_requirements:
	aws s3 cp requirements.txt s3://dev-gametaco-airflow/
	sleep 10
	$(eval LATEST_REQUIREMENTS_VERSION := $(shell aws s3api list-object-versions --bucket dev-gametaco-airflow --prefix requirements.txt --query 'Versions[?IsLatest].[VersionId]' --output text))
	AWS_PAGER="" aws mwaa update-environment --name dev-gametaco-airflow --requirements-s3-object-version $(LATEST_REQUIREMENTS_VERSION) --requirements-s3-path requirements.txt