"""
Project # 1 
Name: Oliver Xu
Time to completion:
Comments:

Sources:
https://docs.python.org/3/library/csv.html
"""

# You are welcome to add code outside of functions
# like imports and other statements
import csv


def read_csv_string(input_):
    """
    Takes a string which is the contents of a CSV file.
    Returns an object containing the data from the file.
    The specific representation of the object is up to you.
    The data object will be passed to the write_*_string functions.
    """
    
    raise NotImplementedError()


def write_csv_string(data):
    """
    Takes a data object (created by one of the read_*_string functions).
    Returns a string in the CSV format.
    """
    csv_list = []
    csv_dict = {}
    raise NotImplementedError()


def read_json_string(input_):
    """
    Similar to read_csv_string, except works for JSON files.
    """
    raise NotImplementedError()


def write_json_string(data):
    """
    Writes JSON strings. Similar to write_csv_string.
    """
    raise NotImplementedError()


def read_xml_string(input_):
    """
    You should know the drill by now...
    """
    raise NotImplementedError()


def write_xml_string(data):
    """
    Feel free to write what you want here.
    """
    raise NotImplementedError()



# The code below isn't needed, but may be helpful in testing your code.
if __name__ == "__main__":
    input_ = """
    col1,col2,col3
    1,2,3
    4,5,6
    """
    expected = """
    [{"col1": "1", "col2": "2", "col3": "3"}, {"col1": "4", "col2": "5", "col3": "6"}]
    """

    def super_strip(input_):
        """
        Removes all leading/trailing whitespace and blank lines
        """
        lines = []
        for line in input_.splitlines():
            stripped = line.strip()
            if stripped:
                lines.append(stripped)
        return "\n".join(lines) + "\n"

    input_ = super_strip(input_)
    expected = super_strip(expected)

    print("Input:")
    print(input_)
    print()
    data = read_csv_string(input_)
    print("Your data object:")
    print(data)
    print()
    output = write_json_string(data)
    output = super_strip(output)
    print("Output:")
    print(output)
    print()
