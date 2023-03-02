'''
Created on 12. 11. 2018

@author: esner
'''
import os
import unittest
from pathlib import Path

import mock
from freezegun import freeze_time

from component import Component


class TestComponent(unittest.TestCase):

    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {'KBC_DATADIR': './non-existing-dir'})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    @mock.patch.dict(os.environ,
                     {'KBC_DATADIR': Path(__file__).parent.parent.joinpath('component_config/sample-config').as_posix()})
    def test_conditions_added_on_incremental_and_without(self):
        comp = Component()
        comp.date_from = "from"
        comp.date_to = "to"
        res = comp._update_conditions_with_incremental_options('condition', 'incremental')
        self.assertEqual(res, 'condition,incremental;GE;from,incremental;LE;to')

        res = comp._update_conditions_with_incremental_options('condition', None, )
        self.assertEqual(res, 'condition')


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
