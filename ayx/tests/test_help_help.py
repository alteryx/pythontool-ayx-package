from unittest import TestCase
from IPython.core.display import Markdown
from ayx.Alteryx import help as helpFunction


class TestHelpDoesntFail(TestCase):

    def testHelpFunction2(self):
        try:
            helpFunction()
        except:
            self.fail("ayx.Alteryx.help() returned an error!")
