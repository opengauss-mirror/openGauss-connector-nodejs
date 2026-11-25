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
var pg = helper.pg
const suite = new helper.Suite()
const pool = new pg.Pool()

suite.test('can access results when no rows are returned', function (done) {
  var checkResult = function (result) {
    assert(result.fields, 'should have fields definition')
    assert.equal(result.fields.length, 1)
    assert.equal(result.fields[0].name, 'val')
    assert.equal(result.fields[0].dataTypeID, 25)
  }

  pool.connect(
    assert.success(function (client, release) {
      const q = new pg.Query('select $1::text as val limit 0', ['hi'])
      var query = client.query(
        q,
        assert.success(function (result) {
          checkResult(result)
          release()
          pool.end(done)
        })
      )

      assert.emits(query, 'end', checkResult)
    })
  )
})
