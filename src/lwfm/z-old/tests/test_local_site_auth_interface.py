import unittest
from lwfm.base.Site import Site

class TestLocalSiteAuthInterface(unittest.TestCase):
    """
    'write a test case for the local site Auth interface using the Site factory'
    
    Test the login functionality.

    This function tests the login functionality of the system by performing the following steps:
    1. Retrieves an instance of the `Site` class using the `getSiteInstanceFactory` method with the parameter "local".
    2. Gets the authentication driver from the site instance using the `getAuthDriver` method.
    3. Attempts to login using the authentication driver.
    4. Asserts that the login is successful.

    This function does not take any parameters.

    Returns:
        None
    """
    def test_login(self):
        site = Site.getSiteInstanceFactory("local")
        auth_driver = site.getAuthDriver()
        result = auth_driver.login()
        self.assertTrue(result, "Login should be successful")

    def test_isAuthCurrent(self):
        site = Site.getSiteInstanceFactory("local")
        auth_driver = site.getAuthDriver()
        result = auth_driver.isAuthCurrent()
        self.assertTrue(result, "Auth should be considered current")

if __name__ == '__main__':
    unittest.main()