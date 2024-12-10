import subprocess


def run_all_scripts():
    try:
        subprocess.run(["python", "FetchData.py"], check=True)

        subprocess.run(["python", "UpToDB.py"], check=True)

        subprocess.run(["python", "MailService.py"], check=True)

        print("All scripts ran successfully.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    run_all_scripts()