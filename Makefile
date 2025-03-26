pyenv: .python-version

.python-version: requirements/test.txt
	if [ -z "`pyenv virtualenvs | grep enginator`" ]; then\
	    pyenv virtualenv enginator;\
	fi
	if [ ! -f .python-version ]; then\
	    pyenv local enginator;\
	fi
	pip install -r requirements/test.txt
	touch .python-version

test: pyenv
	pytest --cov=enginator -vv tests/ --doctest-modules enginator --without-integration --without-slow-integration

integration: pyenv
	pytest --cov=enginator -vv tests/ --doctest-modules enginator --with-integration --with-slow-integration

clean:
	pyenv virtualenv-delete enginator

spellcheck:
	codespell -S "*.json" enginator docs/*rst tests templates *.rst

check:
	pre-commit run --all-files
