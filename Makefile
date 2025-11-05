.SILENT:

.PHONY: all

predictions:
	@Rscript initial_script.R

rawdata:
	@Rscript load_data.R