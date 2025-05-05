import glob

def main():

    # List out tasks
    tasks = glob.glob("tasks/*.py")

    # Execute tasks
    for task in tasks:
        with open(task) as f:
            code = f.read()
            print(f"Executing {task}")
            exec(code)

if __name__ == "__main__":
    main()