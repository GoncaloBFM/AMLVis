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

NATIONALITIES = ['Spain', 'Canada', 'Australia']

TRANSACTIONS_PREFIX = 't'
ACCOUNT_PREFIX = 'a'
PERSON_PREFIX = 'p'
PERSON_ACCOUNT_OWNERSHIP_PREFIX = 'pa'
COMPANY_PREFIX = 'e'
COMPANY_ACCOUNT_OWNERSHIP_PREFIX = 'ea'
PERSON_COMPANY_OWNERSHIP_PREFIX = 'pe'

def main():
    fake = Faker()
    transactions = pandas.read_csv(DEFAULT_DATASET_PATH, names=COLS, skiprows=1)
    transactions = transactions[transactions['originAccount'] != transactions['targetAccount']]
    transactions = transactions[transactions['transactionType'] != 'Reinvestment']
    transactions = transactions.drop(columns=['amountReceived', 'currencyReceived', 'originBank', 'targetBank'])

    transactions['timestamp'] = transactions['timestamp'].apply(lambda x: datetime.strptime(x, '%Y/%m/%d %H:%M').timestamp())

    old_account_ids = list(set(transactions['originAccount']) | set(transactions['targetAccount']))
    account_ids = list(map(lambda x: f'{ACCOUNT_PREFIX}{x}', range(len(old_account_ids))))
    accounts = pandas.DataFrame({'id:ID': account_ids})
    accounts[':LABEL'] = 'account'
    accounts['type'] = 'account'
    accounts['iban'] = [fake.iban() for _ in account_ids]
    accounts['nationality'] = [random.choice(NATIONALITIES) for _ in account_ids]
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
    transactions['type'] = 'transaction'
    transactions['currency'] = 'USD'
    transactions['id'] = [f'{TRANSACTIONS_PREFIX}{edge_number}' for edge_number in range(len(transactions))]

    print('Transactions finished')
    person_ids = random.sample(list(map(lambda x: f'{PERSON_PREFIX}{x}', list(range(len(account_ids))))), int(len(account_ids) * .6))
    persons = pandas.DataFrame({
        'id:ID': person_ids,
        'name': [fake.name() for _ in person_ids],
        'nationality': [random.choice(NATIONALITIES) for _ in person_ids],
        'address': [fake.street_address() for _ in person_ids],
    })
    persons[':LABEL'] = 'person'
    persons['type'] = 'person'

    print('Persons finished')

    company_ids = random.sample(list(map(lambda x: f'{COMPANY_PREFIX}{x}', list(range(len(account_ids))))), int(len(account_ids) * .1))
    companies = pandas.DataFrame({
        'id:ID': company_ids,
        'name': [fake.company() for _ in company_ids],
        'nationality': [random.choice(NATIONALITIES) for _ in company_ids],
        'address': [fake.street_address() for _ in company_ids],
    })

    companies[':LABEL'] = 'company'
    companies['type'] = 'company'

    print('Companies finished')

    persons_accounts = account_ids[:int(len(account_ids) * .9)]
    companies_accounts = account_ids[-int(len(account_ids) * .1):]

    persons_with_accounts = itertools.cycle(person_ids)
    persons_accounts_relations = pandas.DataFrame({
        'source:START_ID': [next(persons_with_accounts) for _ in persons_accounts],
        'target:END_ID': persons_accounts,
        'id': [f'{PERSON_ACCOUNT_OWNERSHIP_PREFIX}{edge_number}' for edge_number in range(len(persons_accounts))],
    })
    persons_accounts_relations['type'] = 'connection'
    persons_accounts_relations['name'] = 'has'
    persons_accounts_relations['directed'] = True

    print('Persons - accounts relations finished')

    companies_with_accounts = itertools.cycle(company_ids)
    companies_accounts_relations = pandas.DataFrame({
        'source:START_ID': [next(companies_with_accounts) for _ in companies_accounts],
        'target:END_ID': companies_accounts,
        'id': [f'{COMPANY_ACCOUNT_OWNERSHIP_PREFIX}{edge_number}' for edge_number in range(len(companies_accounts))],
    })
    companies_accounts_relations['type'] = 'connection'
    companies_accounts_relations['name'] = 'has'
    companies_accounts_relations['directed'] = True

    print('Companies - accounts relations finished')

    persons_companies_relations = pandas.DataFrame({
        'source:START_ID': person_ids[:len(company_ids)],
        'target:END_ID': company_ids,
        'id': [f'{PERSON_COMPANY_OWNERSHIP_PREFIX}{edge_number}' for edge_number in range(len(company_ids))],
    })
    persons_companies_relations['type'] = 'connection'
    persons_companies_relations['name'] = 'owns'
    persons_companies_relations['directed'] = True

    print('Persons - companies relations finished')

    temp_accounts_file_path = os.path.join(DATA_DIRECTORY, 'accounts.tmp')
    temp_persons_file_path = os.path.join(DATA_DIRECTORY, 'persons.tmp')
    temp_companies_file_path = os.path.join(DATA_DIRECTORY, 'companies.tmp')
    temp_transactions_file_path = os.path.join(DATA_DIRECTORY, 'transactions.tmp')
    temp_persons_accounts_relations_file_path = os.path.join(DATA_DIRECTORY, 'persons_accounts_relations.tmp')
    temp_companies_accounts_relations_file_path = os.path.join(DATA_DIRECTORY, 'companies_accounts_relations.tmp')
    temp_persons_companies_accounts_relations_file_path = os.path.join(DATA_DIRECTORY, 'persons_companies_relations.tmp')

    accounts.to_csv(temp_accounts_file_path, index=False)
    companies.to_csv(temp_companies_file_path, index=False)
    transactions.to_csv(temp_transactions_file_path, index=False)
    persons.to_csv(temp_persons_file_path, index=False)
    persons_accounts_relations.to_csv(temp_persons_accounts_relations_file_path, index=False)
    companies_accounts_relations.to_csv(temp_companies_accounts_relations_file_path, index=False)
    persons_companies_relations.to_csv(temp_persons_companies_accounts_relations_file_path, index=False)

    NODES_PARAMETER = '--nodes='
    RELATIONSHIP_PARAMETER = '--relationships='
    NEO4J_COMMAND_PATH = 'neo4j-admin'
    BASE_PARAMETERS = 'database import full --overwrite-destination'.split(' ')

    full_command = [NEO4J_COMMAND_PATH] + \
                   BASE_PARAMETERS + \
                   [NODES_PARAMETER + temp_accounts_file_path] + \
                   [NODES_PARAMETER + temp_persons_file_path] + \
                   [NODES_PARAMETER + temp_companies_file_path] + \
                   [RELATIONSHIP_PARAMETER + temp_transactions_file_path] + \
                   [RELATIONSHIP_PARAMETER + temp_persons_accounts_relations_file_path] + \
                   [RELATIONSHIP_PARAMETER + temp_companies_accounts_relations_file_path] + \
                   [RELATIONSHIP_PARAMETER + temp_persons_companies_accounts_relations_file_path] + \
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