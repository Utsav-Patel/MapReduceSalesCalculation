# How to generate results?
1. Our repo is publicly available so you can clone repo at your desired location.
2. Change Project Path in constants.py file.
3. You also need to append path in each file .py file or you can add your project path to .bash_rc so that your python can find your project.
4. Also, make sure you have installed all required dependencies. You can install dependency using `pip install -r requirements.txt`.
5. Now, you have all things to run our project. 
6. From the main directory of the project, you can run this command to execute our scripts: `python filepath.py main.py`
7. Above command runs MapReduce Job with `inline` runner so your process will be run on a single core. 
8. If you want to run MapReduce Job with multiple core, you need to run process with one extra argument which is `--runner local`. 
9. Above command runs MapReduce Job locally by utilizing all available cores. You can specifically mention number of mapper or number of reducer using `jobconf` argument by mentioning it to MRSteps.
