import sys

from src.recentchangessearch import main_entry

if __name__ == "__main__":
    # print(sys.argv[1:])
    # sys.exit(0)

    sys.exit(main_entry(sys.argv[1:]))
