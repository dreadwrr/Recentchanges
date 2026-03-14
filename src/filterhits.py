import re
import csv
import importlib.util
from collections import defaultdict
from .configfunctions import find_install
install_root = find_install()
filter_patterns_path = install_root / "filter.py"
spec = importlib.util.spec_from_file_location("user_filter", filter_patterns_path)
user_filter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(user_filter)


def update_filter_csv(RECENT, csv_file, escaped_user):

    hits_dict = defaultdict(int)

    try:
        with open(csv_file, newline='') as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            for row in reader:
                pattern, count = row
                hits_dict[pattern] = int(count)
    except FileNotFoundError:
        pass

    patterns = user_filter._filter

    for pattern_literal in patterns:

        pattern = pattern_literal.replace("{{user}}", escaped_user)

        regex = re.compile(pattern)

        count = sum(1 for line in RECENT if len(line) >= 2 and regex.search(line[1]))
        hits_dict[pattern_literal] += count

    # add patterns not matched to csv
    for pattern_literal in patterns:
        hits_dict.setdefault(pattern_literal, 0)

    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Entry", "Hits"])
        for pattern_literal in patterns:
            writer.writerow([pattern_literal, hits_dict.get(pattern_literal, 0)])
