import itertools
import os
import random
import subprocess
from datetime import datetime

import pandas
from faker import Faker

from definitions import DEFAULT_DATASET_PATH, DATA_DIRECTORY

DATABASE_NAME = ''
COLS = ['timestamp', 'originBank', 'originAccount', 'targetBank', 'targetAccount', 'amountReceived',
        'currencyReceived', 'amountPaid', 'currencyPaid', 'transactionType', 'isMl']

def main():
    fake = Faker()
    transactions = pandas.read_csv(DEFAULT_DATASET_PATH, names=COLS, skiprows=1)
    transactions = transactions[transactions['originAccount'] != transactions['targetAccount']]
    transactions = transactions[transactions['transactionType'] != 'Reinvestment']
    transactions = transactions.drop(columns=['amountReceived', 'currencyReceived', 'originBank', 'targetBank'])

    transactions['timestamp'] = transactions['timestamp'].apply(lambda x: datetime.strptime(x, '%Y/%m/%d %H:%M').timestamp())

    old_account_ids = list(set(transactions['originAccount']) | set(transactions['targetAccount']))
    account_ids = list(map(lambda x: f'a{x}', range(len(old_account_ids))))
    accounts = pandas.DataFrame({'id:ID': account_ids})
    accounts[':LABEL'] = 'account'
    accounts['type'] = 'account'
    accounts['bank'] = [fake.bank() for _ in accounts],
    accounts['iban'] = [fake.iban() for _ in accounts],
    account_ids_map = {old_account_id: new_account_id for old_account_id, new_account_id in zip(old_account_ids, account_ids)}

    print('Accounts finished')

    transactions['originAccount'] = transactions['originAccount'].map(account_ids_map)
    transactions['targetAccount'] = transactions['targetAccount'].map(account_ids_map)
    transactions['isMl'] = transactions['isMl'].map({0: False, 1: True})

    transactions = transactions.rename(columns={
        'originAccount':'source:START_ID',
        'targetAccount':'target:END_ID',
        'amountPaid': 'amountPaid:float',
        'isMl': 'isMl:boolean',
        'timestamp': 'timestamp:float',
    })
    transactions['type:TYPE'] = 'transaction'
    transactions['id'] = [f't{edge_number}' for edge_number in range(len(transactions))]

    print('Transactions finished')

    person_ids = random.sample(list(map(lambda x: f'p{x}', list(range(len(account_ids))))), int(len(account_ids) * .3))
    persons = pandas.DataFrame({
        'id:ID': person_ids,
        'name': [fake.name() for _ in person_ids],
        'country': [fake.country() for _ in person_ids],
        'address': [fake.street_address() for _ in person_ids],
        'birthDate': [fake.date_of_birth() for _ in person_ids]
    })
    persons[':LABEL'] = 'person'
    persons['type'] = 'person'

    print('Persons finished')

    company_ids = random.sample(list(map(lambda x: f'e{x}', list(range(len(account_ids))))), int(len(account_ids) * .3))
    companies = pandas.DataFrame({
        'id:ID': company_ids,
        'name': [fake.company() for _ in person_ids],
        'country': [fake.country() for _ in person_ids],
        'address': [fake.street_address() for _ in person_ids],
    })
    companies[':LABEL'] = 'company'
    companies['type'] = 'company'

    print('Companies finished')

    accounts_for_companies = account_ids[:len(company_ids)]
    accounts_companies_relation = pandas.DataFrame({
        'source:START_ID': accounts_for_companies,
        'target:END_ID': company_ids,
        'id': [f'ce{edge_number}' for edge_number in range(len(account_ids))],
    })
    accounts_companies_relation['name'] = 'employee'
    accounts_companies_relation['directed'] = True

    accounts_for_persons = account_ids[:len(company_ids)]
    persons_accounts_relation = itertools.cycle(accounts_for_persons)
    relations = pandas.DataFrame({
        'source:START_ID': [next(persons_accounts_relation) for _ in account_ids],
        'target:END_ID': account_ids,
        'id': [f'cc{edge_number}' for edge_number in range(len(account_ids))],
    })
    relations['type:TYPE'] = 'connection'
    persons_accounts_relation['name'] = 'person'

    print('Relations finished')

    temp_accounts_file_path = os.path.join(DATA_DIRECTORY, 'accounts.tmp')
    temp_persons_file_path = os.path.join(DATA_DIRECTORY, 'persons.tmp')
    temp_transactions_file_path = os.path.join(DATA_DIRECTORY, 'transactions.tmp')
    temp_relations_file_path = os.path.join(DATA_DIRECTORY, 'relations.tmp')

    accounts.to_csv(temp_accounts_file_path, index=False)
    transactions.to_csv(temp_transactions_file_path, index=False)
    persons.to_csv(temp_persons_file_path, index=False)
    relations.to_csv(temp_relations_file_path, index=False)

    NODES_PARAMETER = '--nodes='
    RELATIONSHIP_PARAMETER = '--relationships='
    NEO4J_COMMAND_PATH = 'neo4j-admin'
    BASE_PARAMETERS = 'database import full --overwrite-destination'.split(' ')

    full_command = [NEO4J_COMMAND_PATH] + \
                   BASE_PARAMETERS + \
                   [NODES_PARAMETER + temp_accounts_file_path] + \
                   [NODES_PARAMETER + temp_persons_file_path] + \
                   [RELATIONSHIP_PARAMETER + temp_transactions_file_path] + \
                   [RELATIONSHIP_PARAMETER + temp_relations_file_path] + \
                   [DATABASE_NAME]

    print('Running command:')
    print(' '.join(full_command))
    print()

    # import_to_neo4j = subprocess.Popen(' '.join(full_command),
    #                                    stdin=subprocess.PIPE,
    #                                    stdout=subprocess.PIPE,
    #                                    stderr=subprocess.PIPE,
    #                                    shell=True)
    # output, error = import_to_neo4j.communicate()
    # output = output.decode('UTF-8 ')
    # error = error.decode('UTF-8')
    # if output:
    #     print('output:')
    #     print(output)
    #     print()
    # if error:
    #     print('error:')
    #     print(error)
    #     print()

    # os.remove(temp_accounts_file_path)
    # os.remove(temp_transactions_file_path)
    # os.remove('./import.report')

main()