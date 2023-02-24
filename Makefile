all: publish

catalog:
	python common/catalog.py

publish: catalog
	aws --endpoint-url https://minio.ninja s3 cp --cache-control "public, max-age=64600" catalog.json s3://data.followthegrant.org/
