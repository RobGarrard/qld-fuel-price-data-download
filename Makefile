.Phony: env price-lambda non-price-lambda

bucket=qld-fuel-prices

env:
	python -m venv env/
	env/bin/pip install -r requirements.txt

################################################################################
# Builid AWS lambda

price-lambda:
	# Check if build directory exists, if not make it.
	mkdir -p ./build

	# Download python packages to local directory
	env/bin/pip install -r src/price-data-lambda/requirements.txt \
						-t src/price-data-lambda/lib/;
	echo "Installed Python libraries to local directory.";

	# Copy the python script into the directory
	cp src/price-data-lambda/price-data-lambda.py \
	   src/price-data-lambda/lib/

	# Zip the file and place it in build/
	cd src/price-data-lambda/lib/ && \
	zip -r ../../../build/price-data-lambda.zip *;

	# Remove lib dir
	@echo "\nCleaning up installed packages"
	rm -r src/price-data-lambda/lib/

	@echo "\nZip file for lambda located in build/";

non-price-lambda:
	# Check if build directory exists, if not make it.
	mkdir -p ./build

	# Download python packages to local directory
	env/bin/pip install -r src/non-price-data-lambda/requirements.txt \
						-t src/non-price-data-lambda/lib/;
	echo "Installed Python libraries to local directory.";

	# Copy the python script into the directory
	cp src/non-price-data-lambda/non-price-data-lambda.py \
	   src/non-price-data-lambda/lib/

	# Zip the file and place it in build/
	cd src/non-price-data-lambda/lib/ && \
	zip -r ../../../build/non-price-data-lambda.zip *;

	# Remove lib dir
	@echo "\nCleaning up installed packages"
	rm -r src/non-price-data-lambda/lib/

	@echo "\nZip file for lambda located in build/";

################################################################################
# Sync raw-data to local fs.

download-raw-data:
	aws s3 sync s3://${bucket}/raw-data/ tmp/raw-data/

upload-processed-data:
	aws s3 sync tmp/processed-data/ s3://${bucket}/processed-data/ --include *.parquet --exclude *.crc