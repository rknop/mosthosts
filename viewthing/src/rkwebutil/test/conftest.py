# This file is part of rkwebutil
#
# rkwebutil is Copyright 2023 by Robert Knop
#
# rkwebutil is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.
#
# rkwebutil is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License
# along with rkwebutil. If not, see <https://www.gnu.org/licenses/>.

import time
import requests
import pytest

import psycopg2
import psycopg2.extras

@pytest.fixture(scope='module')
def database():
    conn = psycopg2.connect( host='postgres', user='postgres', password='fragile', dbname='test_rkwebutil',
                             cursor_factory=psycopg2.extras.RealDictCursor )
    return conn

