# -*- coding: utf-8 -*-
##############################################################################
#
#    Copyright (C) 2016 credativ Ltd <http://www.credativ.co.uk>
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as published
#    by the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
##############################################################################

from datetime import datetime
import pycountry
from cdf2_format import fmt
import csv
import StringIO
from collections import defaultdict


def cdf_number_to_float(number, flag='D', dp=0):
    sign = (flag == 'D') and 1.0 or -1.0
    return int(number) * sign / (10 ** dp)


class CdfParser(object):
    statement_map = None
    statements = None
    from_date = False
    to_date = False
    ref = False
    orm = False

    def __init__(self, orm):
        self.statement_map = defaultdict(dict)
        self.statements = []
        self.from_date = False
        self.to_date = False
        self.ref = False
        self.orm = orm

    def validate_cdf2(self, csvfile):
        try:
            header = csvfile.next()
            trailer = csvfile.next()
            assert len(header) == 14, 'Header length incorrect'
            assert header[0] == '0000', 'Header type incorrect'
            assert len(trailer) == 10, 'Trailer length incorrect'
            assert trailer[0] == '0000', 'Trailer type incorrect'
        except:
            raise ValueError('Not a CDF2 file')
        return True

    def parse(self, data):
        data_stream = StringIO.StringIO(data)
        csvfile = csv.reader(data_stream)
        self.validate_cdf2(csvfile)
        csvfile = csv.reader(data_stream)  # We need to rewind the stream
        cdf_data = self.data_to_cdf(csvfile)
        self.parse_financial_transaction(cdf_data)
        if not self.statement_map:
            raise ValueError('No statements found in file')
        for stmt in self.statement_map.values():
            if 'transactions' in stmt:
                self.statements.append(stmt)
            if not stmt.get('balance_start', 0) and \
                    not stmt.get('balance_end', 0):
                stmt['balance_end'] = stmt['total']
                stmt['balance_end_real'] = stmt['total']
            if 'total' in stmt:
                stmt.pop('total')
        return self.statements

    def data_to_cdf(self, data):
        records = []
        try:
            for line in data:
                try:
                    record = {}
                    line = [x.strip() for x in line]

                    # Get the overall msgtype
                    msgtypes = fmt.get(line[0], [])

                    # Get the specific msgtype if more than one
                    valid_types = []
                    for msgtype in msgtypes:
                        if len(line) == len(msgtype['Schema'].keys()):
                            valid_types.append(msgtype)

                    if not valid_types:
                        raise NotImplementedError(
                            "Unable to process message type %s "
                            "with fields %s" % (line[0], len(line)))
                    if len(valid_types) > 1:
                        raise NotImplementedError(
                            "Multiple formats (%s) for message type %s with "
                            "fields %s" % (len(valid_types), line[0],
                                           len(line)))

                    msgschema = valid_types[0]

                    # Parse according to rule
                    record['Data'] = {}
                    record['Format'] = msgschema['Name']
                    for col, colschema in msgschema['Schema'].iteritems():
                        record['Data'][(col, colschema['Name'])] = line[col-1]

                    records.append(record)
                except NotImplementedError, e:
                    continue
        except StopIteration, e:
            pass
        return records

    def parse_financial_transaction(self, data):
        for row in data:
            if row['Format'] == 'Customer (Transmission)  Header':
                # Header
                self.handle_header(row)
            elif row['Format'] == 'Account Address Maintenance':
                # Start a new statement
                self.handle_account_record(row)
            elif row['Format'] == 'Financial Transaction':
                # New transaction
                self.handle_transaction_record(row)
            else:
                continue
        return

    def handle_header(self, record):
        d = record['Data']
        self.from_date = datetime.strptime(d[(9, 'Providing from Date')],
                                           '%Y%m%d %H:%M:%S')
        self.to_date = datetime.strptime(d[(10, 'Providing to Date')],
                                         '%Y%m%d %H:%M:%S')
        self.ref = d[(12, 'File Reference Number')]

    def handle_account_record(self, record):
        d = record['Data']
        # Find or create a new statement
        stmt = self.statement_map[d[(10, 'AccountNumber')]]

        local_currency = pycountry.currencies.get(
            numeric=d[(35, 'CurrencyCode')]).letter
        # Populate statement details
        stmt['balance_start'] = cdf_number_to_float(
            d[(38, 'PreviousBalance')],
            flag=d[(37, 'PreviousBalanceSign')], dp=4)
        stmt['balance_end'] = cdf_number_to_float(
            d[(40, 'EndingBalance')],
            flag=d[(39, 'EndingBalanceSign')], dp=4)
        stmt['balance_end_real'] = stmt['balance_end']

        stmt['date'] = datetime.strptime(d[(36, 'StatementDate')], '%Y%m%d')
        stmt['account_number'] = d[(10, 'AccountNumber')]
        stmt['currency_code'] = local_currency
        stmt['name'] = '%s-%s' % (stmt['account_number'],
                                  stmt['date'].strftime('%Y-%m-%d'),)
        if 'total' not in stmt:
            stmt['total'] = 0

    def populate_account_record_from_transaction(self, record, stmt):
        d = record['Data']

        local_currency = pycountry.currencies.get(
            numeric=d[(27, 'PostedCurrencyCode')]).letter

        stmt['date'] = datetime.strptime(d[(16, 'PostingDate')], '%Y%m%d')
        stmt['account_number'] = d[(9, 'AccountNumber')]
        stmt['currency_code'] = local_currency
        stmt['name'] = '%s-%s' % (stmt['account_number'],
                                  stmt['date'].strftime('%Y-%m-%d'),)
        if 'total' not in stmt:
            stmt['total'] = 0

    def handle_transaction_record(self, record):
        d = record['Data']
        # Find or create a new statement
        stmt = self.statement_map[d[(9, 'AccountNumber')]]
        # Append a transaction
        trnx = {}
        stmt.setdefault('transactions', []).append(trnx)

        if 'statement_id' not in stmt:
            self.populate_account_record_from_transaction(record, stmt)

        trnx['date'] = datetime.strptime(
            d[(16, 'PostingDate')], '%Y%m%d')
        trnx['amount'] = cdf_number_to_float(
            d[(15, 'TransactionAmount')], dp=4)
        trnx['ref'] = d[(11, 'AcquirerReference Number')]
        trnx['name'] = ' '.join([d[(19, 'MerchantName')],
                                 d[(20, 'MerchantCity')],
                                 d[(38, 'MerchantLocationPostalCode')],
                                 d[(22, 'MerchantCountry')]])

        partner = self.orm.env['res.partner'].\
            search([('name', 'ilike', d[(19, 'MerchantName')])])

        # Search for partner
        if len(partner) == 1:
            trnx['partner_id'] = partner.id or partner[0].id
            banks = partner.bank_ids
        # If partner search return multiple records search on bank
        else:
            banks = self.orm.env['res.partner.bank'].\
                search([('owner_name', '=', d[(19, 'MerchantName')])])
        if banks:
            bank_account = banks[0]
            trnx['bank_account_id'] = bank_account.id

        stmt['total'] += trnx['amount']
