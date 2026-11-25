/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
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
