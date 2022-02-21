import csv

def format_schema_to_dict(data):
    with open('./schema.csv', newline='') as f:
        schema = list(csv.reader(f))[0]

    data_dict = {schema[idx]:data.split(",")[idx] for idx in range(len(schema))}

    return data_dict


def output_to_file(data, file_name):
    f = open("{file_name}.csv".format(file_name=file_name), "a")
    f.write(data)
    f.close()


if __name__ == '__main__':
    format_schema_to_dict()