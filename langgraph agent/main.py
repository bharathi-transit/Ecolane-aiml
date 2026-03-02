import os
import sys


def main():
    project_root = os.path.dirname(os.path.abspath(__file__))
    src_path = os.path.join(project_root, "src")
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    from pbi_agent.cli import main as cli_main

    cli_main()


if __name__ == "__main__":
    main()
