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
// http://developer.postgresql.org/pgdocs/postgres/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

test('flushing once', function () {
  helper.connect(function (con) {
    con.parse({
      text: 'select * from ids',
    })

    con.bind()
    con.execute()
    con.flush()

    assert.emits(con, 'parseComplete')
    assert.emits(con, 'bindComplete')
    assert.emits(con, 'dataRow')
    assert.emits(con, 'commandComplete', function () {
      con.sync()
    })
    assert.emits(con, 'readyForQuery', function () {
      con.end()
    })
  })
})

test('sending many flushes', function () {
  helper.connect(function (con) {
    assert.emits(con, 'parseComplete', function () {
      con.bind()
      con.flush()
    })

    assert.emits(con, 'bindComplete', function () {
      con.execute()
      con.flush()
    })

    assert.emits(con, 'dataRow', function (msg) {
      assert.equal(msg.fields[0], 1)
      assert.emits(con, 'dataRow', function (msg) {
        assert.equal(msg.fields[0], 2)
        assert.emits(con, 'commandComplete', function () {
          con.sync()
        })
        assert.emits(con, 'readyForQuery', function () {
          con.end()
        })
      })
    })

    con.parse({
      text: 'select * from ids order by id',
    })

    con.flush()
  })
})
