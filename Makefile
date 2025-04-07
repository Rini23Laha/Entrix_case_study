.PHONY: init deploy test clean

init:
	python -m venv .venv
	source .venv/bin/activate && pip install -r requirements.txt

deploy:
	source .venv/bin/activate && cdk bootstrap && cdk deploy --all --require-approval never

test:
	source .venv/bin/activate && pytest tests/

clean:
	rm -rf .venv cdk.out .pytest_cache
