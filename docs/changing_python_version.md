# record of bash commands to run to switch to correct python version

pyenv install 3.12.6

pyenv local 3.12.6

# then:

- check that .python-version shows "3.12.16"
- delete the .venv folder in the repo
- run:
poetry lock
poetry install
