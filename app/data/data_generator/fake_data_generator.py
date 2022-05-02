import csv
from faker import Faker
from time import time
from tqdm import tqdm

RECORD_COUNT = 100000000
fake = Faker()

def create_csv_file():
    with open('../fake_100M.csv', 'w', newline='') as csvfile:
        fieldnames = ['region', 'origin_coord', 'destination_coord', 'datetime', 'datasource']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        with tqdm(total=RECORD_COUNT) as pbar:
            for i in range(RECORD_COUNT):
                writer.writerow(
                    {
                        'region': fake.state(),
                        'origin_coord': fake.email(),
                        'destination_coord': fake.random_int(min=100, max=199),
                        'datetime': fake.random_int(min=1, max=9),
                        'datasource': fake.sentence()
                    }
                )
                pbar.update(RECORD_COUNT)

if __name__ == '__main__':
    start = time()
    create_csv_file()
    elapsed = time() - start
    print('created csv file time: {}'.format(elapsed))