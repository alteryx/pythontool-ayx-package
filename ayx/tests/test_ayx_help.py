from unittest import TestCase
from ayx import Alteryx


class TestAlteryxHelp(TestCase):

    def testHelp(self):
        try:
            Alteryx.help()
        except:
            self.fail('Unable to run Alteryx.help() !')
