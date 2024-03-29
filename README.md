# Battery Module Assembly (BMA) Output Teams 📈

<img width="180" alt="Teams Logo" src="https://media.github.tesla.com/user/12490/files/f557b2f3-6f99-434a-b913-f6f2e2757713">

## Brief Description
BMA output teams is a repository that uses webhooks to send massive amounts of reports to team channels. Common channels where this script can be seen in work are in ‘M3Y Battery Leadership’ as ‘Zone 1 Updates, Zone 2 Updates, etc.’ Any sort of pybot that can be seen in the channels within this team can likely be viewed as something ran under `BMA_output_teams`.

``` mermaid
pie title Repository Composition
    "Hourly Updates" : 10
    "Alert Updates" : 4
    "Staffing Updates" : 1
    "Misc Updates": 1
```

 This repository includes not only report generation but also statistic summarization, regionalized area metrics and more. It is common to see many files represent specific zones or other miscellaneous areas. The primary purpose of teams is to provide the people on production with near real-time statistics to identify possible bottlenecks to maximize efficiency.  The timing and schedules for each individual ‘update’ are listed in `main.py` as a job. Most posts are made every hour with some exceptions.


### (Relevant) Program Terminology
* EOS
    * End-Of-Shift
* Webhooks 
    * Webhooks are automated messages sent from apps when something happens. They have a message—or payload—and are sent to a unique URL (a teams-specific url in this case).

### (Relevant) Notes
* It is important to consider shift and date when performing calculation around data that (may) involve multiple shifts.
* In `main.py`, all scheduler functions are wrapped around an error-listener function (`scheduler.add_listener(listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)`) that reports details tracebacks and errors, if an error indeed occurs.
    * In similar nature, it is equally as important when you are writing your code or contributing to implement descriptive error handling. All error handling that gets ‘logged’ (`logging.info`, `logging.error`, etc.) is ingested into splunk, which is the main point-of-contact for troubleshooting issues. 
    * A general rule of thumb for error-handling is to be more explicit than implicit.
* With all connections established via `get_sql_conn()`. ensure that they are also closed, via <connection_name_here>.close()
* When writing functions is also considerate to write docstring per each function to help pass the torch (make it easier for other developers in the future to understand the codebase).


## Installation

If help is needed with external developer setup see this [onboarding guide](https://confluence.teslamotors.com/pages/viewpage.action?spaceKey=PRODENG&title=GFNV+Developers+Onboarding).

If not, continue following this guide for local testing:

- First go to [Fuze](https://console.platform.tesla.com/) and go to 'Secrets'.

<img width="234" alt="Screenshot 2024-03-04 at 12 23 14 PM" src="https://media.github.tesla.com/user/12490/files/896748f9-682e-4c98-88d6-efac19e959c0">

Then, copy `gf1pe-bm-creds` credentials and paste locally into your developer workspace where `bmaoutput` is located. 

If you are having any trouble with getting the local-credentials you can play around with `helper_functions.py` to get it to pick up your credentials (*FYI* never commit these files. It should be under a `.gitignore`)

Once done, go to `main.py` and temporarily set `env='dev'` such that you are running though `dev` locally. 

Now create an environment.
- Use either `python -m venv <environment dir>` or `Ctrl` + `Shift` + `P` --> 'Create Virtual Environment' for those using vscode, and activate the environment for the way you completed this. 
- Next, run `pip install -r requirements.txt` in the root directory to fetch and install all program dependencies.
- Now, run your program via `python main.py`

The below graph should illustrate how a developer-based branch should be setup for anyone making contributions:
* Start by creating your own branch that is based off of the main branch.
* Contribute your changes to your own branch and then make a pull request to merge it back to the main branch.

``` mermaid
gitGraph
    commit
    commit
    branch dev_branch
    checkout dev_branch
    commit
    commit
    checkout main
    merge dev_branch
    commit
    commit
```

## Latest Script Contributors

See github commits for latest contributors.

Current maintiners are as follows:

- [@nshanmugam](https://github.tesla.com/nshanmugam)
- [@mberlied](https://github.tesla.com/mberlied)
- [@apuliyaneth](https://github.tesla.com/apuliyaneth)
- [@mquien](https://github.tesla.com/mquien)
- [@avancil](https://github.tesla.com/avancil)

