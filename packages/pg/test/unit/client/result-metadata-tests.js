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

var testForTag = function (tagText, callback) {
  test('includes command tag data for tag ' + tagText, function () {
    var client = helper.client()
    client.connection.emit('readyForQuery')

    var query = client.query(
      'whatever',
      assert.calls((err, result) => {
        assert.ok(result != null, 'should pass something to this event')
        callback(result)
      })
    )
    assert.lengthIs(client.connection.queries, 1)

    client.connection.emit('commandComplete', {
      text: tagText,
    })

    client.connection.emit('readyForQuery')
  })
}

var check = function (oid, rowCount, command) {
  return function (result) {
    if (oid != null) {
      assert.equal(result.oid, oid)
    }
    assert.equal(result.rowCount, rowCount)
    assert.equal(result.command, command)
  }
}

testForTag('INSERT 0 3', check(0, 3, 'INSERT'))
testForTag('INSERT 841 1', check(841, 1, 'INSERT'))
testForTag('DELETE 10', check(null, 10, 'DELETE'))
testForTag('UPDATE 11', check(null, 11, 'UPDATE'))
testForTag('SELECT 20', check(null, 20, 'SELECT'))
testForTag('COPY', check(null, null, 'COPY'))
testForTag('COPY 12345', check(null, 12345, 'COPY'))
