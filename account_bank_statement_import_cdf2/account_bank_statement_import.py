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
import logging
from openerp import api, models, fields, _
from cdf2 import CdfParser as Parser

_logger = logging.getLogger(__name__)


class AccountBankStatementImport(models.TransientModel):
    """Add parsing of Mastercard CDF 2 files to bank statement import."""
    _inherit = 'account.bank.statement.import'

    @api.model
    def _parse_file(self, data_file):
        """Parse a Mastercard CDF 2 file."""
        parser = Parser(self)
        try:
            _logger.debug("Try parsing with Mastercard CDF 2.")
            return parser.parse(data_file)
        except ValueError, e:
            # Returning super will call next candidate:
            _logger.debug("Statement file was not a Mastercard CDF 2 file.",
                          exc_info=True)
            return super(AccountBankStatementImport, self)._parse_file(
                data_file)
