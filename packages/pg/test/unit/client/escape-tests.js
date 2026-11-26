/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/mit
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict'
var helper = require('./test-helper')

function createClient(callback) {
  var client = new Client(helper.config)
  client.connect(function (err) {
    return callback(client)
  })
}

var testLit = function (testName, input, expected) {
  test(testName, function () {
    var client = new Client(helper.config)
    var actual = client.escapeLiteral(input)
    assert.equal(expected, actual)
  })
}

var testIdent = function (testName, input, expected) {
  test(testName, function () {
    var client = new Client(helper.config)
    var actual = client.escapeIdentifier(input)
    assert.equal(expected, actual)
  })
}

testLit('escapeLiteral: no special characters', 'hello world', "'hello world'")

testLit('escapeLiteral: contains double quotes only', 'hello " world', "'hello \" world'")

testLit('escapeLiteral: contains single quotes only', "hello ' world", "'hello '' world'")

testLit('escapeLiteral: contains backslashes only', 'hello \\ world', " E'hello \\\\ world'")

testLit('escapeLiteral: contains single quotes and double quotes', 'hello \' " world', "'hello '' \" world'")

testLit('escapeLiteral: contains double quotes and backslashes', 'hello \\ " world', " E'hello \\\\ \" world'")

testLit('escapeLiteral: contains single quotes and backslashes', "hello \\ ' world", " E'hello \\\\ '' world'")

testLit(
  'escapeLiteral: contains single quotes, double quotes, and backslashes',
  'hello \\ \' " world',
  " E'hello \\\\ '' \" world'"
)

testIdent('escapeIdentifier: no special characters', 'hello world', '"hello world"')

testIdent('escapeIdentifier: contains double quotes only', 'hello " world', '"hello "" world"')

testIdent('escapeIdentifier: contains single quotes only', "hello ' world", '"hello \' world"')

testIdent('escapeIdentifier: contains backslashes only', 'hello \\ world', '"hello \\ world"')

testIdent('escapeIdentifier: contains single quotes and double quotes', 'hello \' " world', '"hello \' "" world"')

testIdent('escapeIdentifier: contains double quotes and backslashes', 'hello \\ " world', '"hello \\ "" world"')

testIdent('escapeIdentifier: contains single quotes and backslashes', "hello \\ ' world", '"hello \\ \' world"')

testIdent(
  'escapeIdentifier: contains single quotes, double quotes, and backslashes',
  'hello \\ \' " world',
  '"hello \\ \' "" world"'
)
