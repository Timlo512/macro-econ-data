import glob
import argparse

parser = argparse.ArgumentParser(description="Execute Python tasks from files in the 'tasks' directory.")
parser.add_argument('--tasks', type=str, help='Specific task files to execute (optional).', nargs='*')
args = parser.parse_args()

def main():

    # Get task argument
    task_args = args.tasks
    print(task_args)

    # List out tasks
    tasks = glob.glob("tasks/*.py")

    # Execute tasks
    for task in tasks:
        check = any([task_name in task for task_name in task_args]) if task_args else True
        if check:
            with open(task) as f:
                code = f.read()
                print(f"Executing {task}")
                exec(code)

if __name__ == "__main__":
    main()