import argparse
import csv
import matplotlib.pyplot as plt


def main(args: argparse.Namespace):
    with open(args.input_file) as csvfile:
        reader = csv.reader(csvfile)
        row = next(reader)
        x = list(map(lambda i: int(i), row[1:]))

        for row in reader:
            y = list(map(
                lambda i: int(i) if i != '' else float('nan'),
                row[1:]
            ))
            plt.plot(x, y, label=row[0])

    plt.xlabel('# of updates')
    plt.ylabel('Time taken (ms)')
    plt.title(args.title)
    plt.legend()
    plt.savefig(args.output_file)


parser = argparse.ArgumentParser()
parser.add_argument('input_file')
parser.add_argument('output_file')
parser.add_argument('--title', type=str, required=True)

args = parser.parse_args()
main(args)
