from dataclasses import dataclass
from datetime import datetime
from typing import List

from dataclass_csv import dateformat, DataclassReader

from pathlib import Path

from dataclasses_avroschema import AvroModel


@dataclass
@dateformat('%Y-%m-%d')
class PersonRecord(AvroModel):
    "Definition of Person record model"
    id: int
    firstname: str
    lastname: str
    email: str
    profession: str
    city: str
    country: str
    random_date: datetime

    class Meta:
        namespace = "Person.v1"
        aliases = ["person-v1", "person-record"]


def read_data_from_csv(resources_path: Path) -> List[PersonRecord]:

    result = []

    print(f'resources path is {resources_path.resolve()}')

    files_gen = resources_path.glob("**/*")

    files_list = [
        str(elem)
        for elem in files_gen
        if elem.is_file()
    ]

    print(f'files list is {files_list}')

    for elem in files_list:

        with open(elem) as f:
            csv_reader = DataclassReader(f, PersonRecord)
            for row in csv_reader:
                result.append(row)

    return result


if __name__ == '__main__':

    resources_path = Path(__file__).resolve().parents[2] / Path("resources")

    result = read_data_from_csv(resources_path)

    print(result[0:6])
