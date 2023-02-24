all: clean process publish

data/src:
	mkdir -p data/src
	wget --inet4-only -P data/src/ -r -l1 -H -nd -N -np -A "ZIP" -e robots=off https://www.cms.gov/OpenPayments/Data/Dataset-Downloads
	wget --inet4-only -P data/src/ -r -l1 -H -nd -N -np -A "ZIP" -e robots=off https://www.cms.gov/openpayments/archived-datasets

data/export/entities.ftm.json: data/src
	python parse.py
	mkdir -p data/export
	ftm store iterate -d us_cms_openpayments > data/export/entities.ftm.json

publish:
	bash ../../upload.sh us_cms_openpayments data/export

process: data/export/entities.ftm.json

clean:
	rm -rf data/
