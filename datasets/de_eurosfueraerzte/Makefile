# all: clean process publish
all: publish

# data/src:
# 	mkdir -p data/src
# 	aws s3 --endpoint-url https://minio.ninja sync s3://data.followthegrant.org/de_eurosfueraerzte/src data/src

# data/fragments.json: data/src
# 	python parse.py

# data/sorted.json: data/fragments.json
# 	sort -o data/sorted.json data/fragments.json

# data/export/entities.ftm.json: data/sorted.json
# 	mkdir -p data/export
# 	nk sorted-aggregate -i data/sorted.json -o data/export/entities.ftm.json

publish:
	python catalog.py
	bash ../../upload.sh de_eurosfueraerzte data/export

# process: data/export/entities.ftm.json

# clean:
# 	rm -rf data/
