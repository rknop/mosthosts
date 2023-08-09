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

import re
import time
import json
import requests
import pytest

import selenium
from selenium.common.exceptions import NoSuchElementException
import selenium.webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.remote.webelement import WebElement

class AuthTestBase:
    url = None
    
    @pytest.fixture(scope='class')
    def browser( self ):
        opts = selenium.webdriver.FirefoxOptions()
        opts.add_argument( "--headless" )
        ff = selenium.webdriver.Firefox( options=opts )
        yield ff
        ff.close()
        ff.quit()

    @pytest.fixture(scope='class')
    def load_frontpage( self, browser ):
        browser.get( self.url )
        el = WebDriverWait( browser, timeout=10 ).until( lambda d: d.find_element(By.CLASS_NAME, 'link') )
        assert el.text == 'Request Password Reset'

    def findresetpasswordbutton( self, browser ):
        try:
            statusdiv = browser.find_element( By.ID, 'status-div' )
            button = statusdiv.find_element( By.TAG_NAME, 'button' )
            if button.text != 'Email Password Reset Link':
                return None
            return button
        except Exception as e:
            return None

    @pytest.fixture(scope='class')
    def click_password_reset( self, browser, load_frontpage ):
        statusdiv = browser.find_element( By.ID, 'status-div' )
        p = statusdiv.find_element( By.TAG_NAME, 'p' )
        assert p.text == 'Request Password Reset'
        p.click()
        button = WebDriverWait( browser, timeout=10 ).until( self.findresetpasswordbutton )
        return button


    def waitforresetemailsent( self, browser ):
        try:
            statusdiv = browser.find_element( By.ID, 'status-div' )
            p = statusdiv.find_element( By.TAG_NAME, 'p' )
            if p.text == 'Password reset link(s) sent for test.':
                return p
            else:
                return None
        except Exception as ex:
            return None

    @pytest.fixture(scope='class')
    def password_reset_link( self, database, browser, click_password_reset ):
        statusdiv = browser.find_element( By.ID, 'status-div' )
        username = statusdiv.find_element( By.ID, 'login_username' )
        username.clear()
        username.send_keys( 'test' )
        click_password_reset.click()
        p = WebDriverWait( browser, timeout=10 ).until( self.waitforresetemailsent )
        assert p.text == 'Password reset link(s) sent for test.'

        # This is ugly; I should find a better way to wait for mailhog to get the mail
        time.sleep(2)

        res = requests.get( 'http://mailhog:8025/api/v2/messages' )
        assert res.status_code == 200
        blob = json.loads( res.text )
        assert len( blob['items'] ) > 0
        # Empirically, the first item is the most recent
        msg = blob['items'][0]
        hdrs = msg['Content']['Headers']
        assert hdrs['From'] == ['rkwebutil test <nobody@nowhere.org>']
        assert hdrs['To'] == ['testuser@mailhog']
        assert hdrs['Subject'] == ['rkwebutil test password reset']
        body = msg['Content']['Body']
        match = re.search( f"^Somebody requested.*({self.url}auth/resetpassword\?uuid=[0-9a-f\-]+)$",
                           body, flags=re.DOTALL )
        assert match is not None

        yield match.group( 1 )

        cursor = database.cursor()
        cursor.execute( "DELETE FROM passwordlink" )
        database.commit()

    @pytest.fixture(scope='class')
    def reset_password( self, database, browser, password_reset_link ):
        browser.get( password_reset_link )
        time.sleep(3)
        el = WebDriverWait( browser, timeout=10 ).until( lambda d: d.find_element(By.ID, 'resetpasswd_linkid' ) )
        assert el.get_attribute('name') == 'linkuuid'
        assert el.tag_name == 'input'
        pw = browser.find_element( By.ID, 'reset_password' )
        conf = browser.find_element( By.ID, 'reset_confirm_password' )
        button = browser.find_element( By.ID, 'setnewpassword_button' )
        pw.clear()
        pw.send_keys( 'gratuitous' )
        conf.clear()
        conf.send_keys( 'gratuitous' )
        # I don't know why, but the alert timeout expired if I didn't sleep before clicking
        time.sleep(1)
        button.click()
        alert = WebDriverWait( browser, timeout=10 ).until( expected_conditions.alert_is_present() )
        assert alert.text == 'Password changed'
        alert.dismiss()

        yield True

        cursor = database.cursor()
        cursor.execute( "UPDATE authuser SET pubkey=NULL,privkey=NULL WHERE username='test'" )
        database.commit()

    def lookforloggedin( self, browser ):
        try:
            statusdiv = browser.find_element( By.ID, 'status-div' )
            p = statusdiv.find_element( By.TAG_NAME, 'p' )
            if p.text[0:29] == "Logged in as test (Test User)":
                return p
            else:
                return None
        except Exception as ex:
            return None

    @pytest.fixture(scope='class')
    def get_frontpage_after_reset_password( self, browser, reset_password ):
        browser.get( self.url )
        el = WebDriverWait( browser, timeout=10 ).until( lambda d: d.find_element(By.CLASS_NAME, 'link') )
        assert el.text == 'Request Password Reset'

    @pytest.fixture(scope='class')
    def login( self, browser ):
        browser.get( self.url )
        el = WebDriverWait( browser, timeout=10 ).until( lambda d: d.find_element(By.CLASS_NAME, 'link') )
        assert el.text == 'Request Password Reset'
        statusdiv = browser.find_element( By.ID, 'status-div' )
        username = statusdiv.find_element( By.ID, 'login_username' )
        password = statusdiv.find_element( By.ID, 'login_password' )
        button = statusdiv.find_element( By.TAG_NAME, 'button' )
        username.clear()
        username.send_keys( 'test' )
        password.clear()
        password.send_keys( 'gratuitous' )
        time.sleep( 1 )
        button.click()
        el = WebDriverWait( browser, timeout=10 ).until( self.lookforloggedin )

    @pytest.fixture(scope='class')
    def logout( self, browser, login ):
        statusdiv = browser.find_element( By.ID, 'status-div' )
        p = statusdiv.find_element( By.TAG_NAME, 'p' )
        span = statusdiv.find_element( By.TAG_NAME, 'span' )
        span.click()
        # This is overkill, but give it time to go through the whole reload
        # I should probably put a wait after this, but, eh.
        time.sleep(5)

    def check_login_prompt( self, browser ):
        inputelems = ( 'login_username', 'login_password' )
        for elem in [ 'login_username', 'login_password', 'status-div', 'main-div' ]:
            e = browser.find_element( value=elem )
            assert type(e) == WebElement
            if elem in inputelems:
                assert e.tag_name == 'input'
            else:
                assert e.tag_name == 'div'
            assert e.is_displayed()

    def test_frontpage( self, browser, load_frontpage ):
        assert browser.title == 'RKWebutil Test'
        with pytest.raises( NoSuchElementException ):
            browser.find_element( value='this_element_does_not_exist' )
        self.check_login_prompt( browser )

    def test_nosuchuser( self, browser, load_frontpage ):
        statusdiv = browser.find_element( By.ID, 'status-div' )
        username = statusdiv.find_element( By.ID, 'login_username' )
        button = statusdiv.find_element( By.TAG_NAME, 'button' )
        username.clear()
        username.send_keys( 'no_such_user' )
        time.sleep(1)
        button.click()
        alert = WebDriverWait( browser, timeout=10 ).until( expected_conditions.alert_is_present() )
        assert alert.text == "No such user no_such_user"
        alert.dismiss()

    def test_no_password( self, browser, load_frontpage ):
        statusdiv = browser.find_element( By.ID, 'status-div' )
        username = statusdiv.find_element( By.ID, 'login_username' )
        password = statusdiv.find_element( By.ID, 'login_password' )
        button = statusdiv.find_element( By.TAG_NAME, 'button' )
        username.clear()
        password.clear()
        username.send_keys( 'test' )
        password.send_keys( 'nothing to see here' )
        time.sleep(1)
        button.click()
        alert = WebDriverWait( browser, timeout=10 ).until( expected_conditions.alert_is_present() )
        assert alert.text == "User test does not have a password set yet."
        alert.dismiss()

    def test_request_reset( self, browser, click_password_reset ):
        email_button = click_password_reset
        statusdiv = browser.find_element( value='status-div' )
        for elem in ( 'login_username', 'login_email' ):
            e = statusdiv.find_element( value=elem )
            assert type(e) == WebElement
            assert e.tag_name == 'input'
            assert e.is_displayed()

    def test_reset_email( self, browser, password_reset_link, database ):
        match = re.search( f'^{self.url}auth/resetpassword\?uuid=([0-9a-f\-]+)$', password_reset_link )
        assert match is not None
        cursor = database.cursor()
        cursor.execute( "SELECT l.id FROM passwordlink l INNER JOIN authuser a ON l.userid=a.id "
                        "WHERE a.username='test' ORDER BY expires DESC" )
        rows = cursor.fetchall()
        # assert len(rows) > 0
        assert len(rows) == 1
        assert rows[0]['id'] == match.group(1)

    def test_reset_password( self, browser, reset_password, database ):
        cursor = database.cursor()
        cursor.execute( "SELECT * FROM authuser WHERE username='test'" )
        rows = cursor.fetchall()
        assert len(rows) == 1
        assert len(rows[0]['pubkey']) > 20
        assert len(rows[0]['privkey']) > 20
        cursor.execute( "SELECT * FROM passwordlink" )
        assert cursor.rowcount == 0

    def test_bad_password( self, browser, get_frontpage_after_reset_password ):
        statusdiv = browser.find_element( By.ID, 'status-div' )
        username = statusdiv.find_element( By.ID, 'login_username' )
        password = statusdiv.find_element( By.ID, 'login_password' )
        button = statusdiv.find_element( By.TAG_NAME, 'button' )
        username.clear()
        password.clear()
        username.send_keys( 'test' )
        password.send_keys( 'this is not the right password' )
        time.sleep(1)
        button.click()
        alert = WebDriverWait( browser, timeout=10 ).until( expected_conditions.alert_is_present() )
        assert alert.text == "Incorrect username or password"
        alert.dismiss()

    def test_login( self, browser, login ):
        statusdiv = browser.find_element( By.ID, 'status-div' )
        p = statusdiv.find_element( By.TAG_NAME, 'p' )
        assert p.text == 'Logged in as test (Test User) — Log Out'

    def test_logout( self, browser, logout ):
        self.check_login_prompt( browser )
        h2 = browser.find_element( By.TAG_NAME, "h2" )
        assert h2.text == "Not Logged In"


class TestWebpyAuth(AuthTestBase):
    url = "http://webserver:8080/ap.py/"

class TestFlaskAuth(AuthTestBase):
    url = "http://flaskserver:8081/"
