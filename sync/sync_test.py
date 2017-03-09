#!/usr/bin/env python
# Copyright 2017 Scraper Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# No docstrings required for tests.
# Tests need to be methods of classes to aid in organization of tests. Using
#   the 'self' variable is not required.
# Too many public methods here means "too many tests", which is unlikely.
# This code is in a subdirectory, but is intended to stand alone, so it uses
#   relative imports.
# pylint: disable=missing-docstring, no-self-use, too-many-public-methods, relative-import

import unittest

import sync


class TestSync(unittest.TestCase):

    def test_docstring_exists(self):
        self.assertIsNotNone(sync.__doc__)
