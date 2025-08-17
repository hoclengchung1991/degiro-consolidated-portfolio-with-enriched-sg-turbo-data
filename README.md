# degiro-consolidated-portfolio-with-enriched-sg-turbo-data
The script assumes you are using the Dutch version of Degiro
## Setup

- Make sure make command works on your machine
- Install python 3.12
- Install poetry and poetry shell
- run `make devenv` in root of this repo
- Create a `.env` file in the root of this repo using `example.env`. Put your username and password in that file. Also put your totp_secret_key in this file. See [here](https://github.com/chavithra/degiro-connector?tab=readme-ov-file#36-how-to-find-your--totp_secret_key-) for details
- Create `.env2` after getting TEMP_MAIL_TOKEN and Rapidd api key from temp-mail-so package.
- Run the `main.py` file using vscode debugger and have it stop just before login. THen have it continue. Somehow degiro detects bots. Wait 3 min and your extract is done. 
- Otherwise try `make unblocksa` and `make summarize`