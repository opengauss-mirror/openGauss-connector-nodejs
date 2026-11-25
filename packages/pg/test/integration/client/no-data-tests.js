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
const suite = new helper.Suite()

suite.test('noData message handling', function () {
  var client = helper.client()

  var q = client.query({
    name: 'boom',
    text: 'create temp table boom(id serial, size integer)',
  })

  client.query(
    {
      name: 'insert',
      text: 'insert into boom(size) values($1)',
      values: [100],
    },
    function (err, result) {
      if (err) {
        console.log(err)
        throw err
      }
    }
  )

  client.query({
    name: 'insert',
    values: [101],
  })

  var query = client.query(
    {
      name: 'fetch',
      text: 'select size from boom where size < $1',
      values: [101],
    },
    (err, res) => {
      var row = res.rows[0]
      assert.strictEqual(row.size, 100)
    }
  )

  client.on('drain', client.end.bind(client))
})
