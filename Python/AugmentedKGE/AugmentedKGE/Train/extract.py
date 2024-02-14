import inspect
import os
import re
import sys


def extract_classes(source):
    """Extracts all the classes defined in the given Python source code."""
    class_re = re.compile(r'^class\s+([^\(:\s]+)', re.MULTILINE)
    return class_re.findall(source)


def write_class_file(class_name, source):
    """Writes the given Python source code to a file with the given class name."""
    filename = class_name + '.py'
    with open(filename, 'w') as f:
        f.write(source)


def extract_class_files(filename):
    """Extracts all the classes defined in the given Python file and saves each class to a separate file."""
    with open(filename, 'r') as f:
        source = f.read()

    class_names = extract_classes(source)
    for class_name in class_names:
        class_re = re.compile(r'^class\s+' + class_name + r'\s*:', re.MULTILINE)
        match = class_re.search(source)
        if match:
            start = match.start()
            end = start + len(inspect.getblock([line for line in source.splitlines()[start:]]))
            write_class_file(class_name, source[start:end])


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python extract_classes.py <filename>')
        sys.exit(1)

    filename = sys.argv[1]
    extract_class_files(filename)
